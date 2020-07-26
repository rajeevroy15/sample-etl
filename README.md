ETL script to process checks
===============



### Installation
Installation is pretty straightforward. 
Just navigate to `etl` directory(root direct of this project).
Then provide permission to shell script which will setup a virtual environment.
```
chmod -rf 744 run.sh input output
./run.sh -p=2
```
The above command will assign the appropriate permissions, do the setup and run the etl job with 2 workers.

#####Other examples  
```
./run.sh -h
or
./run.sh -p=5
```


###Manual setup
```
pyvenv-3.6 venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
### Manual Running the script 
The starting point of script is process.py.
When virtual environment is ready and packages has been installed.
The script can be run by following

``
(venv)$ python process.py -p=5
``

OR

``
(venv)$ python process.py -h 
``

OR

``
(venv)$ python process.py
``

`-p` is an optional parameter, which represents number of process or works to the job. The default workers is 3.

`-h` can be use to see available option params to run the script 