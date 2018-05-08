package main

import (
	"os"
	"io"
	"fmt"
	"sort"
	"time"
	"math"
	"bytes"
	"bufio"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"encoding/json"
	"encoding/binary"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

const IdentPattern = "[a-zA-Z_][a-zA-Z_0-9]+"
const TimeFormat = "2006-01-02T15:04:05"

type config struct {
	seriesReq  [] string
	params     map[string]string
	step       string
	url        string
	user       string
	passwd     string
	database   string
	logLevel   string
	configName string
	timePoints [] string
	maxResultPoints int
	outputFname string
	maxPerSecond int
	checkUnpack bool
	listOnly    bool
	maxSeriesToList int
}


type SerieData struct {
	times []uint32
	values []uint64
	serie string
}


var clog = logrus.New()


func makeConfig() *config {
	return &config{params: make(map[string]string), checkUnpack: false}
}


func setupLogging(level string, output io.Writer) {
	clog.Formatter = new(logrus.TextFormatter)
	switch level {
	case "DEBUG":
		clog.Level = logrus.DebugLevel
	case "INFO":
		clog.Level = logrus.InfoLevel
	case "WARNING":
		clog.Level = logrus.WarnLevel
	case "ERROR":
		clog.Level = logrus.ErrorLevel
	case "PANIC":
		clog.Level = logrus.PanicLevel
	}
	clog.Out = output
}


func parseCfg(cfg *config) {
	clog.Info("Load config from ", cfg.configName)
	file, err := os.Open(cfg.configName)
	if err != nil {
		clog.Fatal(err)
	}
	defer file.Close()

	rr, err := regexp.Compile("^" + IdentPattern + "=.*")
	if err != nil {
		clog.Fatal("Fail to compile pattern")
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 && line[0] != '#' {
			if rr.MatchString(line) {
				parts := strings.SplitN(line, "=", 2)
				if _, alreadyHave := cfg.params[parts[0]]; alreadyHave {
					clog.Fatal("Duplicated key '" + parts[0] + "' in config file")
				}
				cfg.params[parts[0]] = parts[1]
			} else {
				cfg.seriesReq = append(cfg.seriesReq, line)
			}
		}
	}
}


func listAllSeries(cfg *config, conn client.Client) *map[string]bool {
	series := make(map[string]bool)

	for lineno, query := range cfg.seriesReq {
		parts := strings.SplitN(query, " ", 2)
		if len(parts) == 0 {
			clog.Fatal("Error in config file at line ", lineno, " must be in format SERIE_NAME [FILTER_EXPR]")
		}
		if matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", parts[0]) ; !matched {
			clog.Fatal("Error in config file at line ", lineno,
				". Bad serie name, must be in format SERIE_NAME [FILTER_EXPR]")
		}
		sql := "SHOW SERIES FROM " + parts[0]
		if len(parts) == 2 {
			sql += " WHERE " + parts[1]
		}
		clog.Info(sql)
		q := client.NewQuery(sql, cfg.database, "ns")
		if response, err := conn.Query(q); err != nil || response.Error() != nil {
			cerr := err
			if cerr == nil {
				cerr = response.Error()
			}
			clog.Fatal("Error listing time series: ", cerr.Error())
		} else {
			for _, result := range response.Results {
				for _, row := range result.Series {
					for _, seriesList := range row.Values {
						for _, serie := range seriesList {
							series[serie.(string)] = true
						}
					}
				}
			}
		}
	}
	return &series
}


func runSQLToData(sql string, database string, conn client.Client, data *SerieData,
				  startTm int64) (error, int, int64, int64) {

	q := client.NewQuery(sql, database, "s")

	qStartAt := time.Now().UnixNano()
	if response, err := conn.Query(q); err == nil && response.Error() == nil {
		qRunTime := time.Now().UnixNano() - qStartAt
		if len(response.Results) > 1 {
			return errors.New("incorrect responce (2+ results)"), 0, 0, 0
		}

		prevPoints := 0
		if len(response.Results) == 1 {
			result := response.Results[0]

			if len(result.Series) > 1 {
				return errors.New("incorrect responce (2+ series)"), 0, 0, 0
			}

			if len(result.Series) == 1 {
				serie := result.Series[0]

				if len(serie.Columns) != 2 || serie.Columns[0] != "time" || serie.Columns[1] != "sum" {
					return errors.New("incorrect columns '" + fmt.Sprintf("%v", serie.Columns)), 0, 0, 0
				}

				prevPoints = len(serie.Values)

				for _, row := range serie.Values {
					if len(row) != 2 {
						return errors.New(strconv.Itoa(len(row)) + " (must be 2) fields in row"), 0, 0, 0
					}

					if row[1] == nil {
						// no data for this time point
						continue
					}

					timeVl, err1 := row[0].(json.Number).Int64()
					if err1 != nil {
						return errors.New("can't parse time from influx output as int64 " + err1.Error()), 0, 0, 0
					}
					timeVls := timeVl / 1000000000 - startTm
					if timeVls > math.MaxUint32 {
						return errors.New("time if to far in future - can't be represented as uint32"), 0, 0, 0
					}
					data.times = append(data.times, uint32(timeVls))

					dataVl, err2 := row[1].(json.Number).Float64()
					if err2 != nil {
						return errors.New("can't parse data from influx output as float64 " + err2.Error()), 0, 0, 0
					}
					data.values = append(data.values, uint64(dataVl * 1000))
				}
			}
		}
		return nil, prevPoints, qStartAt, qRunTime
	} else {
		cerr := err
		if cerr == nil {
			cerr = response.Error()
		}
		return cerr, 0, 0, 0
	}
}


func mirrorSerie(cfg *config, query string, conn client.Client) (error, *SerieData) {
	clog.Info("Selecting '", query, "' serie")
	data := SerieData{
		times: make([]uint32, 0, cfg.maxResultPoints),
		values: make([]uint64, 0, cfg.maxResultPoints),
	}

	janFirst2017UTC := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	for idx := 0 ; idx < len(cfg.timePoints) - 1 ; idx++ {
		frm := cfg.timePoints[idx]
		to := cfg.timePoints[idx + 1]
		sql := fmt.Sprintf("SELECT sum(value) FROM %s AND time>='%s' AND time<'%s' GROUP BY time(%s)",
							query, frm, to, cfg.step)

		err, numSelected, qStartAt, qRunTime := runSQLToData(sql, cfg.database, conn, &data, janFirst2017UTC)

		if err != nil {
			return errors.New("during '" + sql + "' :" + err.Error()), nil
		}

		switch {
		case cfg.maxPerSecond == 0:
			// no throttle
		case cfg.maxPerSecond == -1:
			// sleep as long as previous sql executed
			time.Sleep(time.Duration(int64(qRunTime)) * time.Nanosecond)
		case cfg.maxPerSecond > 0:
			dtime := float64(time.Now().UnixNano() - qStartAt)
			sleep := float64(numSelected) * 1000000000 / float64(cfg.maxPerSecond) - dtime
			if sleep > 0 {
				time.Sleep(time.Duration(int64(sleep)) * time.Nanosecond)
			}
		}
	}
	return nil, &data
}


func fillConfig(cfg * config) {
	for _, name := range []string{"from", "to", "step"} {
		if  _, hasKey := cfg.params[name] ; !hasKey {
			clog.Fatal("Key '" + name + "' must be in config file")
		}
	}

	cfg.step = cfg.params["step"]

	var from, to time.Time
	if tfrom, err := time.Parse(TimeFormat, cfg.params["from"]) ; err != nil {
		clog.Fatal("Can't parse 'from' date in config file: ", err.Error())
	} else {
		from = tfrom
	}

	if tto, err := time.Parse(TimeFormat, cfg.params["to"]) ; err != nil {
		clog.Fatal("Can't parse 'from' date in config file: ", err.Error())
	} else {
		to = tto
	}

	if to.Sub(from).Seconds() < 1 {
		clog.Fatal("'to' date is before 'from'")
	}

	var step time.Duration
	if tstep, err := time.ParseDuration(cfg.step) ; err != nil {
		clog.Fatal("Can't parse 'step' duration field in config file: ", err.Error())
	} else {
		step = tstep
	}

	maxPoints := 9500
	if maxPtStr, maxPtOk := cfg.params["maxperselect"] ; maxPtOk {
		vl, err := strconv.Atoi(maxPtStr)
		if err != nil {
			clog.Fatal("Wrong 'maxperselect' value. Mast be integer")
		}
		maxPoints = vl
	}

	cfg.maxResultPoints = int(to.Sub(from).Seconds() / step.Seconds()) + 1
	cfg.maxResultPoints += cfg.maxResultPoints / maxPoints + 1

	cfg.maxPerSecond = 0
	if maxPerSecondStr, maxPtOk := cfg.params["maxpersecond"] ; maxPtOk {
		vl, err := strconv.Atoi(maxPerSecondStr)
		if err != nil {
			clog.Fatal("Wrong 'maxpersecond' value. Mast be integer")
		}
		cfg.maxPerSecond = vl
	}

	if cfg.maxPerSecond > 0 {
		if cfg.maxPerSecond < maxPoints {
			clog.Fatal("maxpersecond(=", cfg.maxPerSecond, ") must be >= maxperselect(=", maxPoints, ")")
		}
	} else if cfg.maxPerSecond < -1 {
		clog.Fatal("maxpersecond(=", cfg.maxPerSecond, ") must be >= -1")
	}
	maxStepDuration := int64(maxPoints) * int64(step.Seconds())
	currTime := from

	for currTime.Before(to) {
		cfg.timePoints = append(cfg.timePoints, currTime.Format(TimeFormat) + ".0Z")
		currTime = currTime.Add(time.Duration(maxStepDuration) * time.Second)
	}
	cfg.timePoints = append(cfg.timePoints, to.Format(TimeFormat) + ".0Z")
}


func newConn(cfg *config) client.Client {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     cfg.url,
		Username: cfg.user,
		Password: cfg.passwd,
	})
	if err != nil {
		clog.Fatal("Error creating InfluxDB Client: ", err.Error())
	}
	return conn
}


func selector2SQL(selector string) string {
	parts := strings.Split(selector, ",")
	res := parts[0] + " WHERE "
	for idx, expr := range parts[1:] {
		nameAndVal := strings.SplitN(expr, "=", 2)
		if len(nameAndVal) != 2 {
			clog.Fatal("Incorrect serie selector '" + selector + "'")
		}
		res += "\"" + nameAndVal[0] + "\"" + "='" + nameAndVal[1] + "' "
		if idx != len(parts) - 2 {
			res += "AND "
		}
	}
	return res
}


func packSerie(data *SerieData) []byte {
	bname := []byte(data.serie)
	buff := make([]byte, len(bname) + 1 + 4 + len(data.values) * (8 + 4))
	copy(buff, bname)
	offset := len(bname) + 1
	buff[offset - 1] = byte(0)

	binary.BigEndian.PutUint32(buff[offset: offset + 4], uint32(len(data.values)))
	offset += 4

	for _, vl := range data.values {
		binary.BigEndian.PutUint64(buff[offset: offset + 8], vl)
		offset += 8
	}
	for _, vl := range data.times {
		binary.BigEndian.PutUint32(buff[offset: offset + 4], vl)
		offset += 4
	}
	return buff
}


func unpackSerie(buff *bytes.Buffer) (*SerieData, error) {
	selector, err := buff.ReadString(byte(0))
	if err != nil {
		return nil, errors.New("corrupted data found during array size extranction: " + err.Error())
	}

	if len(selector) == 1 {
		return nil, errors.New("empty serie name")
	}

	dataSize := binary.BigEndian.Uint32(buff.Next(4))
	if buff.Len() < (8 + 4) * int(dataSize) {
		return nil, errors.New("corrupted data found during array data/time extranction")
	}

	data := SerieData{
		times: make([]uint32, 0, dataSize),
		values: make([]uint64, 0, dataSize),
		serie: selector[:len(selector) - 1],
	}

	for i := uint32(0); i < dataSize ; i++ {
		data.values = append(data.values, binary.BigEndian.Uint64(buff.Next(8)))
	}
	for i := uint32(0); i < dataSize ; i++ {
		data.times = append(data.times, binary.BigEndian.Uint32(buff.Next(4)))
	}
	return &data, nil
}


func mapValues(mp *map[string]bool) []string {
	res := make([]string, len(*mp))
	idx := 0
	for val := range *mp {
		res[idx] = val
		idx++
	}
	return res
}


func checkSeriesEQ(s1 *SerieData, s2 *SerieData) bool {
	if s1.serie != s2.serie || len(s1.values) != len(s2.values) {
		return false
	} else {
		for idx := range s1.values {
			if s1.values[idx] != s2.values[idx] || s1.times[idx] != s2.times[idx] {
				return false
			}
		}
	}
	return true
}


func testUnpack(origin *SerieData, rbuff *bytes.Buffer) {
	v, e := unpackSerie(rbuff)
	if e != nil {
		clog.Fatal("Failed to unpack ", e.Error())
	}

	if rbuff.Len() != 0 {
		clog.Fatal("Extra bytes left after unpacking")
	}

	if !checkSeriesEQ(v, origin) {
		clog.Fatal("Unpacking failed")
	}
}


func DoMirror(cfg *config) {

	conn := newConn(cfg)

	series := listAllSeries(cfg, conn)

	selectors := mapValues(series)
	for idx, selector := range selectors {
		if idx == cfg.maxSeriesToList + 1 {
			clog.Debug("...")
			break
		}
		clog.Debug("    ", selector)
	}

	if cfg.listOnly {
		return
	}

	clog.Info("Range would be splitted in to ", len(cfg.timePoints) - 1, " subranges")
	for idx := 0 ; idx < len(cfg.timePoints) - 1 ; idx++ {
		clog.Debug("    ", cfg.timePoints[idx], " - ", cfg.timePoints[idx + 1])
	}

	var outFD *os.File
	if cfg.outputFname != "" {
		outF, err := os.OpenFile(cfg.outputFname, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666)
		if err != nil {
			clog.Fatal("Fail to open output file '", cfg.outputFname, "'. Error:", err.Error())
		}
		defer outF.Close()
		outFD = outF
	} else {
		outFD = nil
	}

	sort.Strings(selectors)

	for _, selector := range selectors {
		startTm := time.Now().UnixNano()
		err, data := mirrorSerie(cfg, selector2SQL(selector), conn)
		if err != nil {
			clog.Fatal("Failed to select data:", err.Error())
		}
		selectTimeMS := (time.Now().UnixNano() - startTm)/1000000

		if len(data.times) == 0 {
			continue
		}
		
		data.serie = selector

		wbuff := packSerie(data)
		clog.Info(fmt.Sprintf("%d points selected for %s in %d ms. Packed into %d bytes",
							  len(data.times), data.serie, selectTimeMS, len(wbuff)))

		if cfg.checkUnpack {
			testUnpack(data, bytes.NewBuffer(wbuff))
		}

		if outFD != nil {
			outFD.Write(wbuff)
		}
	}
}


func parseCLI(version string, cfg *config) {
	app := kingpin.New(os.Args[0], "Influxdb data exporter")
	app.Flag("url", "Server url").Short('U').PlaceHolder("PROTO://IP:PORT").
		Default("http://localhost:8086").StringVar(&cfg.url)
	app.Flag("name", "User name").Short('u').PlaceHolder("NAME").StringVar(&cfg.user)
	app.Flag("password", "User pwd").Short('p').PlaceHolder("PASSWORD").
		StringVar(&cfg.passwd)
	app.Flag("db", "Database").Short('d').PlaceHolder("DATABASE").StringVar(&cfg.database)
	app.Flag("output", "output file").Short('o').PlaceHolder("FILENAME").Default("").
		StringVar(&cfg.outputFname)
	app.Flag("loglevel", "Log level (default = DEBUG)").Short('l').Default("DEBUG").
		EnumVar(&cfg.logLevel, "DEBUG", "INFO", "WARNING", "ERROR", "FATAL")
	app.Flag("check", "Check unpack of data").Short('c').BoolVar(&cfg.checkUnpack)
	app.Flag("list-only", "Only list matched timeseries").Short('L').BoolVar(&cfg.listOnly)
	app.Flag("max-list", "Max series to list").Short('m').Default("25").
		IntVar(&cfg.maxSeriesToList)
	app.Arg("config", "Config file").Required().StringVar(&cfg.configName)
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		clog.Fatal("Fail to parse CLI: ", err.Error())
	}
	clog.Info(cfg.configName)
}


func main() {
	cfg := makeConfig()
	parseCLI("0.0.1", cfg)

	setupLogging(cfg.logLevel, os.Stdout)
	parseCfg(cfg)
	fillConfig(cfg)

	clog.Info(cfg.seriesReq)

	DoMirror(cfg)
}
