all: out data/nyc_taxi.ndjson
	du -h data/*.ndjson

clean:
	rm -rf data/ out/

data:
	mkdir data

out:
	mkdir out

data/nyc_taxi.ndjson: data
	for _ in {1..16}; do cat nyc_taxi_example.ndjson >> data/nyc_taxi.ndjson; done

benchmark: out data/nyc_taxi.ndjson
	for c in {1,2,4,8}; do \
		ab -n 256 -c $${c} \
			-g out/requests_$${c}.dat \
			-p data/nyc_taxi.ndjson \
			"http://0.0.0.0:8888/"; \
		cp out/requests_baseline_$${c}.dat out/requests_baseline.dat; \
		cp out/requests_$${c}.dat out/requests.dat; \
		gnuplot plot_response_time.p; \
		mv response_time.png "out/response_time_c$${c}.png"; \
	done

baseline: out data/nyc_taxi.ndjson
	for c in {1,2,4,8}; do \
		ab -n 256 -c $${c} \
			-g out/requests_baseline_$${c}.dat \
			-p data/nyc_taxi.ndjson \
			"http://0.0.0.0:8888/"; \
	done
