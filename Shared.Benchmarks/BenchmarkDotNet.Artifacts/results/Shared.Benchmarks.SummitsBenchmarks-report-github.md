```

BenchmarkDotNet v0.14.0, macOS Sequoia 15.0 (24A335) [Darwin 24.0.0]
Apple M1, 1 CPU, 8 logical and 8 physical cores
.NET SDK 8.0.204
  [Host]     : .NET 8.0.4 (8.0.424.16909), Arm64 RyuJIT AdvSIMD
  DefaultJob : .NET 8.0.4 (8.0.424.16909), Arm64 RyuJIT AdvSIMD


```
| Method               | Mean     | Error    | StdDev   | Ratio | RatioSD |
|--------------------- |---------:|---------:|---------:|------:|--------:|
| CompareAllPoints     | 21.79 ms | 0.265 ms | 0.235 ms |  1.00 |    0.01 |
| ActivityLengthFilter | 19.56 ms | 0.366 ms | 0.325 ms |  0.90 |    0.02 |
| BoundingBoxFilter    | 17.31 ms | 0.235 ms | 0.196 ms |  0.79 |    0.01 |
