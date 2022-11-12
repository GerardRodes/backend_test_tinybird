# output as png image
set terminal png size 640,300

# save file to "benchmark.png"
set output "response_time.png"

# graph title
set title "response time"

#nicer aspect ratio for image size
set size 1,1

# y-axis grid
set grid y

set yrange [0:200]

#x-axis label
set xlabel "requests"

#y-axis label
set ylabel "response time (ms)"

# plot current response times along the baseline responses
plot "out/requests_baseline.dat" using 9 smooth sbezier with lines title "baseline", "out/requests.dat" using 9 smooth sbezier with lines title "current"
