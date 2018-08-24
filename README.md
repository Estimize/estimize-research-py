Estimize Research
=================

Event Studies
-------------

* [Post-Earnings Surprise](/notebooks/post_earnings_event_study.ipynb)
* [Thru-Earnings WS/Estimize Delta](/notebooks/thru_earnings_ws_estimize_delta_event_study.ipynb)

Getting Started
---------------

### Clone the Estimize Research repository to your local environment

    git clone git@github.com:Estimize/estimize-research-py.git
    cd estimize-research-py


### Setup your Python environment and dependencies

We recommend using `virtualenv` and Python 3.5 (make sure you are NOT using Python 3.6):

    python3 -m pip install --user --upgrade pip
    python3 -m pip install --user virtualenv
    python3 -m virtualenv env
    source env/bin/activate
    pip install -r requirements.txt
    pip install -e .


### Load and cache the data files

Make sure you have the following files added to the `./data` directory:

    .
    ├── data
    │   ├── consensus.csv
    │   ├── estimates.csv
    │   ├── instruments.csv
    │   ├── releases.csv
    │   ├── signal_time_series.csv
    │   └── users.csv

To request the files please contact <sales@estimize.com>.

Run the following command:

    estimize init

It may take some time to load and cache the required CSV files, please be patient.

You can now launch Jupyter Notebook: `env/bin/jupyter notebook`


### Running research notebooks

Launch Jupyter Notebooks: `env/bin/jupyter notebook`

You can open a notebook by navigating to `notebooks` in Jupyter Notebook and clicking on the desired notebook (i.e. `post_earnings_event_study.ipynb`).

Once you have opened a notebook, you can re-generate the notebook results by selecting `Kernel` > `Restart & Run All` from the notebook menu.

### Rendering Notebooks as PDFs

In the notebooks directory run:

`jupyter nbconvert --to pdf --template hidecode <notebook-name>`
