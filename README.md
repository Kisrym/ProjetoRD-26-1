# ProjetoRD-26-1

## Como rodar

Para rodar o projeto, basta instalar as dependências com

```python
pip install -r requirements.txt
```

E rodar o `main.py`:

```
python main.py
```

## WebApp

Existem duas versões do projeto: a nativa por meio de WebApp com Quart e a versão apenas por terminal.
Para rodar com a versão do terminal, basta executar `main.py` com o argumento `--cli`:

```python
python main.py --cli
```

Note que, com isso, as informações de peer (como nome ou namespace) terão que ser configuradas manualmente no arquivo `config.json`