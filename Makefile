.PHONY: clean rebuild

influx_sync: main.go
		CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' .

clean:
		-rm -rf influx_sync

rebuild: clean influx_sync
