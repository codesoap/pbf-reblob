set grid
set ylabel "Size Reduction (%)"
set xlabel "Uncompressed Blob Size Limit"
set logscale x 2
set key reverse top left Left
set ytics 1
set term svg size 600,400 background "#ffffff"
set output "compare.svg"
set yrange [-1:10]
plot \
	"compare.txt" using 1:(100*(1-$2/19716)):xticlabels(1) with  lp lw 3 pt 7 ps 1      lc "#ba19ff" title "Bremen (19MB)", \
	"compare.txt" using 1:(100*(1-$3/19716)):xticlabels(1) with  lp dt 2 lw 3 pt 7 ps 1 lc "#ba19ff" title "Bremen zstd (19MB)", \
	"compare.txt" using 1:(100*(1-$4/198620)):xticlabels(1) with lp lw 3 pt 7 ps 1      lc "#ffba19" title "Serbia (194MB)", \
	"compare.txt" using 1:(100*(1-$5/198620)):xticlabels(1) with lp dt 2 lw 3 pt 7 ps 1 lc "#ffba19" title "Serbia zstd (194MB)", \
	"compare.txt" using 1:(100*(1-$6/847168)):xticlabels(1) with lp lw 3 pt 7 ps 1      lc "#5eff19" title "Czech R. (827MB)", \
	"compare.txt" using 1:(100*(1-$7/847168)):xticlabels(1) with lp dt 2 lw 3 pt 7 ps 1 lc "#5eff19" title "Czech R. zstd (827MB)"
