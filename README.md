Estimize Research
=================

Getting Started
---------------

### Clone the Estimize Research repository to your local environment

    git clone git@github.com:Estimize/estimize-research-py.git
    cd estimize-research-py


### Setup your Python environment and dependencies

We recommend using `virtualenv` and Python 3:

    python3 -m pip install --user --upgrade pip
    python3 -m pip install --user virtualenv
    python3 -m virtualenv env
    source env/bin/activate
    pip install -r requirements.txt

You can now launch Jupyter Notebook: `env/bin/jupyter notebook`


### Running research notebooks

Launch Jupyter Notebooks: `env/bin/jupyter notebook`

You can open a notebook by navigating to `notebooks` in Jupyter Notebook and clicking on the desired notebook (i.e. `post_earnings_event_study.ipynb`).

Once you have opened a notebook, you can re-generate the notebook results by selecting `Kernel` > `Restart & Run All` from the notebook menu.
