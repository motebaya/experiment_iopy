Trying to experiment and dig into Python's synchronous/asynchronous I/O.
The goal is to compare which one is faster in handling bulk task concurrently or not.
However, it turns out to be more complex/tricky than I thought...
because there are quite a few modules that can do that, like multiprocessing, threading, queue, and etc.

and, instead of wasting too much time to meet my needs,
i came to the simple conclusion below that can be easily understood.

- **sync (I/O block)** -> _threadpool_ -> parallel/concurrency run in different thread and in same event loop.
- **async (I/O non block)** -> _asyncio gather_ -> parallel/concurrency run in single thread and in same event loop.
- there's nothing any significant advantage if calling an asynchronous function without parallelism.

for that, i was really curious, so i tested it by calling sync and async functions inside a for loop
10 times, and honestly, there was nothing difference.
it just made me waste a lot of time writing more complex code.

- asyncio python3.10 -> https://docs.python.org/3.10/library/asyncio-task.html
- threadpool python3.10 -> https://docs.python.org/3.10/library/asyncio-task.html
- many help -> https://stackoverflow.com/a/70459437/

In I/O testing, there are a lot of dependencies that make the results vary,
like the Python version (there are many performance testing articles on Python, for example,
Python 3.12, which they say is faster than the previous versions, and so on),
the speed of the HDD/SSD in reading files (especially if the file is over 1GB),
and the internet speed or server speed.

Here, I don't really care about the GIL issue in Python because,
this is just I/O testing, and the Python GIL only applies to CPU testing.
But I also found something interesting, because in Python 3.13 and above, there's no GIL anymore.
cc: https://stackoverflow.com/a/77519536/

I'm trying to take the average results after repeating it several times.

 <details>
 <summary>
   Python 3.10.12 
 </summary>

![demo](src/demo-310.svg)

 </details>
 <details>
 <summary>
   Python 3.11.7
 </summary>

![demo](src/demo-311.svg)

 </details>
 <details>
 <summary>
 Python 3.12.1 
 </summary>

![demo](src/demo-312.svg)

 </details>
 <details>
 <summary>
   Python 3.13.0a2
 </summary>

![demo](src/demo-313.svg)

 </details>

speedtest files: https://downloadtestfile.com/
average internet speed: >=7MBps

## License

This project is licensed under the [MIT License](LICENSE).
