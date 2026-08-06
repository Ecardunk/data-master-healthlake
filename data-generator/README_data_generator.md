# Data Generator

Utilitário para geração de dados **batch** e **streaming**, com suporte a reprodutibilidade por meio de seed, sobrescrita de arquivos existentes, envio de eventos ao Azure Event Hubs e upload de partições locais para o Amazon S3.

---

## Pré-requisitos

Antes de executar os comandos, verifique se:

- O Python está instalado e disponível no terminal.
- O terminal está aberto na raiz do projeto.
- As credenciais e variáveis de ambiente necessárias para o Azure Event Hubs e o Amazon S3 estão configuradas.
- O ambiente virtual do projeto está ativado, quando aplicável.

> Os exemplos abaixo utilizam a sintaxe do PowerShell no Windows.

---

## 1. Instalar as dependências

Instale todas as bibliotecas Python necessárias para executar o gerador de dados:

```powershell
python -m pip install -r .\data-generator\requirements.txt
```

O arquivo `requirements.txt` contém as dependências utilizadas pelo projeto.

---

## 2. Gerar dados batch

Gera os arquivos de dados referentes à data informada:

```powershell
python .\data-generator\main.py --odate 2026-08-06 --seed 42
```

### Parâmetros

| Parâmetro | Descrição |
|---|---|
| `--odate` | Data de referência da partição, no formato `YYYY-MM-DD`. |
| `--seed` | Seed utilizada para tornar a geração de dados reproduzível. |

Ao utilizar a mesma seed e os mesmos parâmetros, o gerador deve produzir os mesmos dados.

---

## 3. Substituir CSVs já gerados

Por padrão, o gerador pode impedir a substituição de arquivos existentes. Para sobrescrever os CSVs da mesma partição, utilize a opção `--overwrite`:

```powershell
python .\data-generator\main.py --odate 2026-08-06 --seed 42 --overwrite
```

> Use `--overwrite` com atenção, pois os arquivos existentes da partição poderão ser substituídos.

---

## 4. Gerar dados streaming

Gera eventos de streaming e os envia ao Azure Event Hubs:

```powershell
python .\data-generator\main.py --streaming --stream-count 10 --seed 42 --send-eventhub
```

### Parâmetros

| Parâmetro | Descrição |
|---|---|
| `--streaming` | Ativa o modo de geração de dados streaming. |
| `--stream-count` | Quantidade de eventos que serão gerados. |
| `--seed` | Seed utilizada para reprodução dos dados gerados. |
| `--send-eventhub` | Envia os eventos gerados para o Azure Event Hubs. |

Neste exemplo, serão gerados e enviados **10 eventos**.

### Configuração necessária

Antes da execução, confirme se as credenciais e configurações do Event Hubs estão disponíveis para a aplicação, como:

- Connection string;
- Namespace;
- Nome do Event Hub;
- Variáveis de ambiente exigidas pelo projeto.

---

## 5. Enviar uma partição local para o Amazon S3

Para enviar ao S3 os arquivos associados a uma data de referência:

```powershell
python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-08-06
```

### Parâmetro

| Parâmetro | Descrição |
|---|---|
| `--odate` | Data da partição local que será enviada ao S3, no formato `YYYY-MM-DD`. |

### Configuração necessária

A máquina deve possuir credenciais válidas para acessar o bucket, por exemplo:

- Perfil configurado pelo AWS CLI;
- Variáveis de ambiente da AWS;
- Credenciais temporárias;
- IAM Role, quando executado em infraestrutura AWS.

---

## Fluxo recomendado

Uma execução completa pode seguir esta ordem:

```powershell
# 1. Instalar dependências
python -m pip install -r .\data-generator\requirements.txt

# 2. Gerar os dados batch
python .\data-generator\main.py --odate 2026-08-06 --seed 42

# 3. Enviar a partição gerada para o S3
python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-08-06

# 4. Gerar e enviar eventos de streaming
python .\data-generator\main.py --streaming --stream-count 10 --seed 42 --send-eventhub
```

---

## Exemplos adicionais

### Gerar outra partição

```powershell
python .\data-generator\main.py --odate 2026-08-07 --seed 42
```

### Gerar novamente a mesma partição

```powershell
python .\data-generator\main.py --odate 2026-08-06 --seed 42 --overwrite
```

### Gerar 100 eventos de streaming

```powershell
python .\data-generator\main.py --streaming --stream-count 100 --seed 42 --send-eventhub
```

---

## Resumo dos comandos

| Operação | Comando |
|---|---|
| Instalar dependências | `python -m pip install -r .\data-generator\requirements.txt` |
| Gerar dados batch | `python .\data-generator\main.py --odate 2026-08-06 --seed 42` |
| Sobrescrever CSVs | `python .\data-generator\main.py --odate 2026-08-06 --seed 42 --overwrite` |
| Gerar e enviar streaming | `python .\data-generator\main.py --streaming --stream-count 10 --seed 42 --send-eventhub` |
| Enviar partição ao S3 | `python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-08-06` |

---

## Solução de problemas

### O comando `python` não foi encontrado

Confirme se o Python está instalado e adicionado à variável de ambiente `PATH`.

No Windows, também pode ser possível utilizar:

```powershell
py --version
```

### Dependência não encontrada

Reinstale as dependências:

```powershell
python -m pip install -r .\data-generator\requirements.txt
```

### Erro de acesso ao Event Hubs

Verifique:

- A connection string;
- O nome do Event Hub;
- As variáveis de ambiente;
- As permissões de envio.

### Erro de acesso ao S3

Verifique:

- As credenciais da AWS;
- A região configurada;
- O nome do bucket;
- As permissões IAM;
- A existência da partição local informada.
