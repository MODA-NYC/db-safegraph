# db-safegraph

## Note: 
+ if you are a programatic user, checkout our sample notebooks under [`examples`](https://github.com/NYCPlanning/db-safegraph/tree/master/examples). [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/NYCPlanning/db-safegraph/blob/master/examples/examples.ipynb)
+ If you are a desktop user and prefer to access data through a desktop GUI app, please check out [cyber duck](https://cyberduck.io/)

## Instructions
1. Install dependencies
```bash
pip3 install -r requirements.txt
```
2. Install cli autocomplete
```bash
./sg --install-completion
```
3. Run a recipes (when you hit <kbd>TAB</kbd> there should be a list of available recipe names, autocomplete is availble here)
```bash 
./sg run [TAB]
```

> **Note**: type `./sg --help` to get a list of available comamnds
```
Usage: sg [OPTIONS] COMMAND [ARGS]...

Options:
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.

  --help                Show this message and exit.

Commands:
  run    Running an Athena recipe in the ./recipes folder
  setup  Install minio and set up the accounts we need
  sync   Syncing a safegraph data source, under the ./_sync folder
```

## Resources
- [Data Science Resources](https://docs.safegraph.com/docs/data-science-resources)
- [Colab - Quantifying Sampling Bias in SafeGraph Patterns](https://colab.research.google.com/drive/1u15afRytJMsizySFqA2EPlXSh3KTmNTQ#sandboxMode=true&scrollTo=WUWFYLLXowUJ)
