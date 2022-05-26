#!/usr/bin/python3

import typer
import os

app = typer.Typer()

env = '''
    if [ -f .env ] 
    then 
        export $(cat .env | sed 's/#.*//g' | xargs) 
    fi
    '''

def complete_name_run(incomplete: str):
    current_path=os.path.dirname(os.path.abspath(__file__))
    files=os.listdir(f'{current_path}/recipes')
    valid_names=[i.split('.')[0] for i in files if '.py' in i]

    completion = []
    for name in valid_names:
        if name.startswith(incomplete):
            completion.append(name)
    return completion

def complete_name_sync(incomplete: str):
    current_path=os.path.dirname(os.path.abspath(__file__))
    files=os.listdir(f'{current_path}/_sync')
    valid_names=[i.split('.')[0] for i in files if '.sh' in i]

    completion = []
    for name in valid_names:
        if name.startswith(incomplete):
            completion.append(name)
    return completion

@app.command()
def run(
        name: str = typer.Option (
            'daily_nyc_poivisits', 
            help="recipe name under ./recipes", 
            autocompletion=complete_name_run
        )
    ):
    """
    Running an Athena recipe in the ./recipes folder
    """

    typer.echo(name)
    command=f'''
    {env}
    python3 recipes/{name}.py
    '''
    os.system(command)

@app.command()
def setup():
    """
    Install minio and set up the accounts we need
    """
    command = f'''
    {env}
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    sudo mv ./mc /usr/bin
    mc config host add sg $SG_S3_ENDPOINT $SG_ACCESS_KEY_ID $SG_SECRET_ACCESS_KEY --api S3v4
    mc config host add rdp $RDP_S3_ENDPOINT $RDP_ACCESS_KEY_ID $RDP_SECRET_ACCESS_KEY --api S3v4
    '''
    os.system(command)

@app.command()
def sync(
        name: str = typer.Option (
                'social_distancing', 
                help="bash script names under ./_sync", 
                autocompletion=complete_name_sync
            )
    ):
    """
    Syncing a safegraph data source, under the ./_sync folder
    Got rid of name. Override with hard-code all.sh
    """
    command = f'''
    {env}
    (cd _sync && ./all.sh)

    '''
    os.system(command)

if __name__ == "__main__":
    app()