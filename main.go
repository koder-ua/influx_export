package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
	"github.com/tysonmote/gommap"
	"gopkg.in/alecthomas/kingpin.v2"
)

const IdentPattern = "[a-zA-Z_][a-zA-Z_0-9]+"
const TimeFormat = "2006-01-02T15:04:05"

type config struct {
	seriesReq       []string
	params          map[string]string
	step            string
	url             string
	user            string
	passwd          string
	database        string
	logLevel        string
	configName      string
	timePoints      []string
	maxResultPoints int
	outputFname     string
	maxPerSecond    int
	listOnly        bool
	maxSeriesToList int
	resume          bool
	resumeOk        bool
	readySeries     map[string]bool
	thCount         int
}

type SerieData struct {
	times  []uint32
	values []uint64
	serie  string
}

var clog = logrus.New()

func makeConfig() *config {
	return &config{params: make(map[string]string)}
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

func parseCfg(cfg *config) error {
	clog.Info("Load config from ", cfg.configName)
	file, err := os.Open(cfg.configName)
	if err != nil {
		clog.Fatal(err)
	}
	defer file.Close()

	rr, err := regexp.Compile("^" + IdentPattern + "=.*")
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 && line[0] != '#' && line[0] != ';' {
			if rr.MatchString(line) {
				parts := strings.SplitN(line, "=", 2)
				if _, alreadyHave := cfg.params[parts[0]]; alreadyHave {
					return errors.New("duplicated key '" + parts[0] + "' in config file")
				}
				cfg.params[parts[0]] = parts[1]
			} else {
				cfg.seriesReq = append(cfg.seriesReq, line)
			}
		}
	}
	return nil
}


func listAllSeriesSimple(cfg *config, conn client.Client) ([]string, error) {
	series := make(map[string]bool)
	for lineno, query := range cfg.seriesReq {
		parts := strings.SplitN(query, " ", 2)
		if len(parts) == 0 {
			return nil, errors.New(
				"Error in config file at serie #" + strconv.Itoa(lineno) +
					" must be in format SERIE_NAME [FILTER_EXPR]")
		}

		if matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", parts[0]); !matched {
			return nil, errors.New("error in config file at line " + strconv.Itoa(lineno) +
				". Bad serie name, must be in format SERIE_NAME [FILTER_EXPR]")
		}

        sql := "SHOW SERIES FROM \"" + parts[0] + "\""
    	if len(parts) == 2 {
    		sql += " WHERE " + parts[1]
    	}

        clog.Info(sql)
        q := client.NewQuery(sql, cfg.database, "ns")
        q.Chunked = true
        totalFound := 0
    	if response, err := conn.Query(q); err != nil || response.Error() != nil {
    		cerr := err
    		if cerr == nil {
    			cerr = response.Error()
    		}
    		return nil, errors.New("error listing time series: " + cerr.Error())
    	} else {
    		for _, result := range response.Results {
    			for _, row := range result.Series {
					for _, seriesList := range row.Values {
						for _, serie := range seriesList {
                            serieLst := strings.Split(serie.(string), ",")
                            sort.Strings(serieLst[1:])
                            series[strings.Join(serieLst, ",")] = true
						}
                        totalFound += len(seriesList)
					}
    			}
    		}
        }
        clog.Info("    found ", totalFound, " series")
	}

    templates := make([]string, 0, len(series))
    for templ, _ :=  range series {
        templates = append(templates, templ)
    }

	return templates, nil
}


func runSQLToData(sql string, database string, conn client.Client, data *SerieData,
	startTm int64) (int, int64, int64, error) {

	q := client.NewQuery(sql, database, "s")

	qStartAt := time.Now().UnixNano()
	if response, err := conn.Query(q); err == nil && response.Error() == nil {
		qRunTime := time.Now().UnixNano() - qStartAt
		if len(response.Results) > 1 {
			return 0, 0, 0, errors.New("incorrect responce (2+ results)")
		}

		prevPoints := 0
		if len(response.Results) == 1 {
			result := response.Results[0]

			if len(result.Series) > 1 {
				return 0, 0, 0, errors.New("incorrect responce (2+ series)")
			}

			if len(result.Series) == 1 {
				serie := result.Series[0]

				if len(serie.Columns) != 2 || serie.Columns[0] != "time" {
					return 0, 0, 0, errors.New("incorrect columns '" + fmt.Sprintf("%v", serie.Columns) + "'")
				}

				prevPoints = len(serie.Values)

				for _, row := range serie.Values {
					if len(row) != 2 {
						return 0, 0, 0, errors.New(strconv.Itoa(len(row)) + " (must be 2) fields in row")
					}

					if row[1] == nil {
						// no data for this time point
						continue
					}

					timeVl, err1 := row[0].(json.Number).Int64()
					if err1 != nil {
						return 0, 0, 0, errors.New("can't parse time from influx output as int64 " + err1.Error())
					}

					if timeVl < startTm {
						return 0, 0, 0, errors.New("time is before startTm")
					}

					timeVls := timeVl - startTm
					if timeVls > math.MaxUint32 {
						return 0, 0, 0, errors.New("time if to far in future - can't be represented as uint32")
					}
					data.times = append(data.times, uint32(timeVls))

					dataVl, err2 := row[1].(json.Number).Float64()
					if err2 != nil {
						return 0, 0, 0, errors.New("can't parse data from influx output as float64 " + err2.Error())
					}
					data.values = append(data.values, uint64(dataVl*1000))
				}
			}
		}
		return prevPoints, qStartAt, qRunTime, nil
	} else {
		cerr := err
		if cerr == nil {
			cerr = response.Error()
		}
		return 0, 0, 0, cerr
	}
}

func mirrorSerie(cfg *config, query string, conn client.Client) (*SerieData, error) {
	data := SerieData{
		times:  make([]uint32, 0, cfg.maxResultPoints),
		values: make([]uint64, 0, cfg.maxResultPoints),
	}

	janFirst2017UTC := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	for idx := 0; idx < len(cfg.timePoints)-1; idx++ {
		frm := cfg.timePoints[idx]
		to := cfg.timePoints[idx+1]

		sql := fmt.Sprintf("SELECT max(value) FROM %s AND time>='%s' AND time<'%s' GROUP BY time(%s)",
			query, frm, to, cfg.step)

		numSelected, qStartAt, qRunTime, err := runSQLToData(sql, cfg.database, conn, &data, janFirst2017UTC)

		if err != nil {
			return nil, errors.New("during '" + sql + "' :" + err.Error())
		}

		switch {
		case cfg.maxPerSecond == 0:
			// no throttle
		case cfg.maxPerSecond == -1:
			// sleep as long as previous sql executed
			time.Sleep(time.Duration(int64(qRunTime)) * time.Nanosecond)
		case cfg.maxPerSecond > 0:
			dtime := float64(time.Now().UnixNano() - qStartAt)
			sleep := float64(numSelected)*1000000000/float64(cfg.maxPerSecond) - dtime
			if sleep > 0 {
				time.Sleep(time.Duration(int64(sleep)) * time.Nanosecond)
			}
		}
	}
	return &data, nil
}

func fillConfig(cfg *config) error {
	for _, name := range []string{"from", "to", "step"} {
		if _, hasKey := cfg.params[name]; !hasKey {
			return errors.New("key '" + name + "' must be in config file")
		}
	}

	cfg.step = cfg.params["step"]

	var from, to time.Time
	if tfrom, err := time.Parse(TimeFormat, cfg.params["from"]); err != nil {
		return errors.New("can't parse 'from' date in config file: " + err.Error())
	} else {
		from = tfrom
	}

	if tto, err := time.Parse(TimeFormat, cfg.params["to"]); err != nil {
		return errors.New("can't parse 'from' date in config file: " + err.Error())
	} else {
		to = tto
	}

	if to.Sub(from).Seconds() < 1 {
		return errors.New("'to' date is before 'from'")
	}

	var step time.Duration
	if tstep, err := time.ParseDuration(cfg.step); err != nil {
		return errors.New("can't parse 'step' duration field in config file: " + err.Error())
	} else {
		step = tstep
	}

	maxPoints := 9500
	if maxPtStr, maxPtOk := cfg.params["maxperselect"]; maxPtOk {
		vl, err := strconv.Atoi(maxPtStr)
		if err != nil {
			return errors.New("wrong 'maxperselect' value. Mast be integer")
		}
		maxPoints = vl
	}

	cfg.maxResultPoints = int(to.Sub(from).Seconds()/step.Seconds()) + 1
	cfg.maxResultPoints += cfg.maxResultPoints/maxPoints + 1

	cfg.maxPerSecond = 0
	if maxPerSecondStr, maxPtOk := cfg.params["maxpersecond"]; maxPtOk {
		vl, err := strconv.Atoi(maxPerSecondStr)
		if err != nil {
			return errors.New("wrong 'maxpersecond' value. Mast be integer")
		}
		cfg.maxPerSecond = vl
	}

	if cfg.maxPerSecond > 0 {
		if cfg.maxPerSecond < maxPoints {
			return errors.New("maxpersecond(=" + strconv.Itoa(cfg.maxPerSecond) +
				") must be >= maxperselect(=" + strconv.Itoa(maxPoints) + ")")
		}
	} else if cfg.maxPerSecond < -1 {
		return errors.New("maxpersecond(=" + strconv.Itoa(cfg.maxPerSecond) + ") must be >= -1")
	}
	maxStepDuration := int64(maxPoints) * int64(step.Seconds())
	currTime := from

	for currTime.Before(to) {
		cfg.timePoints = append(cfg.timePoints, currTime.Format(TimeFormat)+".0Z")
		currTime = currTime.Add(time.Duration(maxStepDuration) * time.Second)
	}
	cfg.timePoints = append(cfg.timePoints, to.Format(TimeFormat)+".0Z")
	cfg.resumeOk = false
	return nil
}

func newConn(cfg *config) (client.Client, error) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     cfg.url,
		Username: cfg.user,
		Password: cfg.passwd,
	})
	if err != nil {
		return nil, errors.New("error creating InfluxDB Client: " + err.Error())
	}
	return conn, nil
}

func selector2SQL(selector string) string {
	parts := strings.Split(selector, ",")
	res := "\"" + parts[0] + "\" WHERE "
	for idx, expr := range parts[1:] {
		nameAndVal := strings.SplitN(expr, "=", 2)
		if len(nameAndVal) != 2 {
			clog.Fatal("Incorrect serie selector '" + selector + "'")
		}
		res += "\"" + nameAndVal[0] + "\"" + "='" + nameAndVal[1] + "' "
		if idx != len(parts)-2 {
			res += "AND "
		}
	}
	return res
}

func packSerie(data *SerieData) []byte {
	bname := []byte(data.serie)
	buff := make([]byte, len(bname)+1+4+len(data.values)*(8+4))
	copy(buff, bname)
	offset := len(bname) + 1
	buff[offset-1] = byte(0)

	binary.BigEndian.PutUint32(buff[offset:offset+4], uint32(len(data.values)))
	offset += 4

	for _, vl := range data.values {
		binary.BigEndian.PutUint64(buff[offset:offset+8], vl)
		offset += 8
	}
	for _, vl := range data.times {
		binary.BigEndian.PutUint32(buff[offset:offset+4], vl)
		offset += 4
	}
	return buff
}

func unpackSerie(buff *bytes.Buffer, unpackData bool) (*SerieData, error) {
	selector, err := buff.ReadString(byte(0))
	if err != nil {
		return nil, errors.New("corrupted data found during name extranction: " + err.Error())
	}

	if len(selector) == 1 {
		return nil, errors.New("empty serie name")
	}

	dataSize := binary.BigEndian.Uint32(buff.Next(4))
	if buff.Len() < (8+4)*int(dataSize) {
		return nil, errors.New("corrupted data found during array data/time extranction")
	}

	data := SerieData{
		serie: selector[:len(selector)-1],
	}

	if unpackData {
		data.times = make([]uint32, 0, dataSize)
		data.values = make([]uint64, 0, dataSize)

		for i := uint32(0); i < dataSize; i++ {
			data.values = append(data.values, binary.BigEndian.Uint64(buff.Next(8)))
		}
		for i := uint32(0); i < dataSize; i++ {
			data.times = append(data.times, binary.BigEndian.Uint32(buff.Next(4)))
		}
		return &data, nil
	} else {
		buff.Next(12 * int(dataSize))
		return &data, nil
	}
}

func allignData(refTimes []uint32, data []uint64, times []uint32, res []uint64) error {
	if len(res) != len(refTimes) {
		return errors.New("result/refTimes size mismatch")
	}

	if len(data) != len(times) {
		return errors.New("data/times size mismatch")
	}

	refMapIdx := 0
	for dataIdx := 0 ; dataIdx < len(data) ; dataIdx++ {
		for refTimes[refMapIdx] < times[dataIdx] {
			res[refMapIdx] = 0
			refMapIdx++
		}
		if refTimes[refMapIdx] != times[dataIdx] {
			return errors.New("unexpected timestamp in times list")
		}
		res[refMapIdx] = data[dataIdx]
	}
	return nil
}

func addDiff(data []uint64, maxDiffVal uint64, minGoodWindowSz int, collector []uint64) error {
	inNoisyPart := false
	noisyStartAt := -1
	cleanStartAt := -1
	if len(collector) != len(data) - 1 {
		return errors.New("Size of collector and data mismatch")
	}

	if minGoodWindowSz < 2 {
		return errors.New("Wrong size of minGoodWindowSz")
	}

	for idx := 1; idx < len(data) ; idx++ {
		diff := data[idx] - data[idx - 1]
		if diff > 0 && diff < maxDiffVal {
			if inNoisyPart {
				if cleanStartAt == -1 {
					cleanStartAt = idx
				} else if idx - cleanStartAt == minGoodWindowSz {
					// end of noisy part, fix it
					noisyDelta := data[cleanStartAt] - data[noisyStartAt]
					if noisyDelta > 0 {
						for noisyStartAt != cleanStartAt {
							cDiff := noisyDelta / uint64(cleanStartAt - noisyStartAt)
							noisyDelta -= cDiff
							collector[noisyStartAt - 1] += noisyDelta
							noisyStartAt++
						}
					}

					inNoisyPart = false
					noisyStartAt = -1
					cleanStartAt = -1
				}
			} else {
				collector[idx - 1] += diff
			}
		} else {
			if inNoisyPart {
				cleanStartAt = -1
			} else {
				inNoisyPart = true
				noisyStartAt = idx
			}
		}
	}
	return nil
}


func findReadySeries(cfg *config) error {
	if cfg.outputFname == "" {
		return errors.New("no output file provided to resume")
	}

	if _, err := os.Stat(cfg.outputFname); os.IsNotExist(err) {
		clog.Info("No output file exists - will start from beginning")
		return nil
	}

	dataFD, err := os.OpenFile(cfg.outputFname, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer dataFD.Close()
	mmap, err := gommap.Map(dataFD.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer mmap.UnsafeUnmap()

	cfg.readySeries = make(map[string]bool)
	buff := bytes.NewBuffer(mmap)
	for buff.Len() > 0 {
		data, err := unpackSerie(buff, false)
		if err != nil {
			return err
		}
		cfg.readySeries[data.serie] = true
	}

	clog.Info("Find ", len(cfg.readySeries), " already processed series. Will skip them")
	cfg.resumeOk = true
	return nil
}

func szToStr(size uint64) string {
	fsize := float64(size)
	prefixes := []string{"B", "KiB", "MiB", "GiB"}
	for _, pref := range prefixes {
		if fsize < 10 {
			return fmt.Sprintf("%.2f %s", fsize, pref)
		} else if fsize < 100 {
			return fmt.Sprintf("%.1f %s", fsize, pref)
		} else if fsize < 10240 {
			return fmt.Sprintf("%d %s", int(fsize), pref)
		}
		fsize /= 1024
	}
	return strconv.Itoa(int(fsize)) + " TiB"
}

func queryThread(selectorQ <-chan string, conn client.Client, results chan<- []byte, cfg *config,
	controlChan chan<- error) {
MainLoop:
	for {
		select {
		case selector := <-selectorQ:
			if "" == selector {
				clog.Info("No more selectors, exiting")
				break MainLoop
			}
			clog.Debug("Selecting '", selector, "' serie")

			startTm := time.Now().UnixNano()
			data, err := mirrorSerie(cfg, selector2SQL(selector), conn)
			if err != nil {
				controlChan <- errors.New("Failed to select data: " + err.Error())
				return
			}
			selectTimeMS := (time.Now().UnixNano() - startTm) / 1000000

			if len(data.times) == 0 {
				results <- nil
				continue
			}

			data.serie = selector
			wbuff := packSerie(data)
			clog.Debug(fmt.Sprintf("%d points selected for %s in %d ms. Packed into %s",
				len(data.times), data.serie, selectTimeMS, szToStr(uint64(len(wbuff)))))
			results <- wbuff
		case <-time.After(1 * time.Second):
			clog.Info("Channel read timeout, exiting")
			break MainLoop
		}
	}
	controlChan <- nil
}

func openOutputFile(cfg *config) (*os.File, error) {
	if cfg.outputFname != "" {
		var mode int
		if cfg.resume && cfg.resumeOk {
			mode = os.O_WRONLY
		} else {
			mode = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		}
		outF, err := os.OpenFile(cfg.outputFname, mode, 0666)
		if err != nil {
			return nil, errors.New("fail to open output file '" + cfg.outputFname + "'. Error: " + err.Error())
		}

		if cfg.resume && cfg.resumeOk {
			outF.Seek(0, 2)
		}
		return outF, nil
	} else {
		return nil, nil
	}
}

func DoMirror(cfg *config) error {
	conn, err1 := newConn(cfg)
	if err1 != nil {
		return err1
	}

	selectors, err2 := listAllSeriesSimple(cfg, conn)
	conn.Close()
	if err2 != nil {
		return err2
	}

	outFD, err3 := openOutputFile(cfg)
	if err3 != nil {
		return err3
	}

	if outFD != nil {
		defer outFD.Close()
	}

	// filter out ready series
	if cfg.resumeOk {
		filteredSelectors := make([]string, 0, len(selectors)-len(cfg.readySeries))
		for _, selector := range selectors {
			if _, ok := cfg.readySeries[selector]; !ok {
				filteredSelectors = append(filteredSelectors, selector)
			}
		}
		selectors = filteredSelectors
	}

	expectedSize := len(selectors) * (cfg.maxResultPoints*12 + 100)
	clog.Info("Will totally select ", len(selectors),
		" series. New data size would be less or close to ", szToStr(uint64(expectedSize)))

    sort.Strings(selectors)
	for idx, selector := range selectors {
		if idx == cfg.maxSeriesToList+1 {
			clog.Debug("...")
			break
		}
		clog.Debug("    ", selector)
	}

	if cfg.listOnly {
		return nil
	}

	clog.Info("Range would be splitted in to ", len(cfg.timePoints)-1, " subranges")
	for idx := 0; idx < len(cfg.timePoints)-1; idx++ {
		clog.Debug("    ", cfg.timePoints[idx], " - ", cfg.timePoints[idx+1])
	}

	sort.Strings(selectors)
	pperc := 0

	stime := time.Now()
	totalSize := uint64(0)

	tasksChan := make(chan string, len(selectors))
	for _, selector := range selectors {
		tasksChan <- selector
	}

	close(tasksChan)

	resultChan := make(chan []byte, cfg.thCount)
	errChan := make(chan error, cfg.thCount)

	conns := make([]client.Client, cfg.thCount)
	for i := 0; i < cfg.thCount; i++ {
		if conn, err := newConn(cfg); err != nil {
			return err
		} else {
			defer conn.Close()
			conns[i] = conn
		}
	}

	for _, thConn := range conns {
		go queryThread(tasksChan, thConn, resultChan, cfg, errChan)
	}

	idx := 0
	runningThreads := cfg.thCount
	for runningThreads > 0 {
		select {
		case wbuff := <-resultChan:
			idx += 1
			cperc := idx * 100 / len(selectors)

			if idx != len(selectors) && cperc > pperc {
				usedS := time.Now().Sub(stime).Nanoseconds() / 1000000000
				secondsLeft := float64(usedS) / float64(idx) * float64(len(selectors)-idx)
				left := time.Duration(int(secondsLeft)) * time.Second
				msg := fmt.Sprintf("%d%% of all series selected. " +
					"Total %s data written. Approx. %v left." +
					" Total sz would be ~%s",
					cperc, szToStr(totalSize), left,
					szToStr(totalSize / uint64(idx) * uint64(len(selectors))))
				clog.Info(msg)
				pperc = cperc
			}

			if wbuff != nil {
				totalSize += uint64(len(wbuff))

				if outFD != nil {
					outFD.Write(wbuff)
				}
			}
		case err := <-errChan:
			if err != nil {
				clog.Error(err.Error())
			}
			runningThreads -= 1
		}
	}

	clog.Info("Finished. Total ", szToStr(totalSize), " of data written")
	return nil
}

func parseCLI(version string, cfg *config) error {
	app := kingpin.New(os.Args[0], "Influxdb data exporter")
	app.Flag("url", "Server url").Short('U').PlaceHolder("PROTO://IP:PORT").
		Default("http://localhost:8086").StringVar(&cfg.url)
	app.Flag("name", "User name").Short('u').PlaceHolder("NAME").StringVar(&cfg.user)
	app.Flag("password", "User pwd").Short('p').PlaceHolder("PASSWORD").
		StringVar(&cfg.passwd)
	app.Flag("db", "Database").Short('d').PlaceHolder("DATABASE").StringVar(&cfg.database)
	app.Flag("output", "output file").Short('o').PlaceHolder("FILENAME").Default("").
		StringVar(&cfg.outputFname)
	app.Flag("log-level", "Log level").Short('l').Default("DEBUG").
		EnumVar(&cfg.logLevel, "DEBUG", "INFO", "WARNING", "ERROR", "FATAL")
	app.Flag("list-only", "Only list matched timeseries").Short('L').BoolVar(&cfg.listOnly)
	app.Flag("max-list", "Max series to list").Short('m').Default("25").
		IntVar(&cfg.maxSeriesToList)
	app.Flag("resume", "Resume previously interrupted operation").Short('r').BoolVar(&cfg.resume)
	app.Flag("threads", "Run in THCOUNT parrallel threads. If not equal to 1 limits are applied separatelly to each thread").
		PlaceHolder("THCOUNT").Short('j').Default("1").IntVar(&cfg.thCount)
	app.Arg("config", "Config file").Required().StringVar(&cfg.configName)
	app.Version(version)
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		return errors.New("fail to parse CLI: " + err.Error())
	}
	return nil
}

func main() {
	cfg := makeConfig()
	if err := parseCLI("0.0.1", cfg); err != nil {
		clog.Error(err.Error())
		os.Exit(1)
	}

	setupLogging(cfg.logLevel, os.Stdout)

	if err := parseCfg(cfg); err != nil {
		clog.Error(err.Error())
		os.Exit(1)
	}
	if err := fillConfig(cfg); err != nil {
		clog.Error(err.Error())
		os.Exit(1)
	}

	if cfg.resume {
		if err := findReadySeries(cfg); err != nil {
			clog.Error("Can't resume: " + err.Error())
			os.Exit(1)
		}
	}
	if err := DoMirror(cfg); err != nil {
		clog.Error(err.Error())
		os.Exit(1)
	}
}
