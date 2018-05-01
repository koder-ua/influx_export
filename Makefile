.PHONY: clean rebuild

influx_sync: main.go
		go build

clean:
		-rm -rf influx_sync

rebuild: clean influx_sync
