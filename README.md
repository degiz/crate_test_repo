# Test assignment for CrateDB

Thanks a lot for the testing task, I had a lot of fun working on it!

## Installation
  
I've used python3 + virtual env, the only external dependency was crate lib

```bash
python3 -m venv ./venv
source ./venv/bin/activate
pip install crate
```

## Running the code

Database connection can be configured with config.ini.

I'm assuming that CrateDB is running in docker on port 4200, and we can use default user "crate".
I'm also including dataset to the git.

In order to run the script, you can use following command:

```bash
python3 main.py -c config.ini -i ./GTFS
```

Some of the dataset files have over 4M lines to insert, that's quite a task for a relational DB.

On my machine with 3,5 GHz Dual-Core Intel Core i7 I've used a threading pool with 4 workers, and got folowwing results:

```
python3 main.py -c config.ini -i ./GTFS  58.78s user 1.79s system 23% cpu 4:12.42 total
``` 

In order to be able to add dataset to github, I had to truncate shapes.txt and stop_times.txt. If you want to test original dataset, feel free to pass the path with "-i" option

Should I continue improving the performance, I'd focus on splitting huge files (over 1M lines) into smaller portions and deal with then in parallel
