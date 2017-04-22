# TPC-H Benchmarks

A set of TPC-H Queries handwritten in C++. These are an imcomplete implementation and may contain
bugs. The schemas used do not attempt to be compatible with the the actual TPC-H benchmark rules.

See http://www.tpc.org/tpch/.

### Usage

[tpch-dbgen](https://github.com/electrum/tpch-dbgen) is the easiest way to create data.

For most queries, generate the data in `./tpch/sf<sf>/`. You might need to hack around a bit to get
it to work (the loading code is fairly straightforward; just look at the `main` function in each C++
file).

Parallelism happens through OpenMP, so you'll need `clang-omp++` on MacOS and `g++` with OpenMP
support on Linux. You can also just run without OpenMP (modify the Makefile to run without the
`-fopenmp` flag and use a compiler of your choice.

Some queries use Intel's AVX2 intrinsics (Haswell and newer architectures support this).

### License

MIT License.




