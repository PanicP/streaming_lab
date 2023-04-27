# Setup in PyCharm

- Open Terinmal using ALT-F12
- Run the following to create a new Conda environment


```console
conda create --prefix ./conda python=3.9
```

- Select local conda environment in PyCharm:
  - CTRL+ALT+S
  - Select Tab Project:
  - Python Interpreter
  - Add Interpreter
  - Add Local Interpreter
  - Select Conda Environment
  - Use existing environment: Select the one you created in your project directory
  - Select the just added environment as Python environment from the list

- run in terminal:

- Close Terminal and open a new Terminal (ALT-F12)

```console
pip install -r requirements.txt
```

- In the Run Configuration Taxi, fill in your Project Details in Parameters and Environment Variables
- In taxi.py edit:
  - table_spec
  -  | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=<YOUR-SUBSCRIPTION>



