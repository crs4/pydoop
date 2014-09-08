
Comparison between "before optimization" binary_streams  and post (will be explained) ::

 zag@pflip ~/work/vs/git/pydoop/test/timings $ python binary_build.py # before opt.
 => write_data: 0.685347795486 s
 => read_data: 0.950889110565 s
 zag@pflip ~/work/vs/git/pydoop/test/timings $ python try_binary.py # after opt
 => write_data: 0.146747112274 s
 => read_data: 0.0607550144196 s
 => read_data(100000): 0.109977960587 s
 => read_data(50000): 0.0551071166992 s

The last two are an intermediate optimization that does not use iterators properly.
