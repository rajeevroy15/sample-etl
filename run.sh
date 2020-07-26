if [ ! -d "venv" ]
then
    pyvenv-3.6 venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi
python process.py $1
