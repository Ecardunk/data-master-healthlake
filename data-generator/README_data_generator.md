# Data Generator

Utilitário para geração de dados **batch** e **streaming**, com perfis explícitos de qualidade, suporte a reprodutibilidade por meio de seed, sobrescrita controlada de arquivos existentes, envio de eventos ao Azure Event Hubs e upload de partições locais para o Amazon S3.

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

Para gerar a partição que deve seguir pelo fluxo até a Gold, use o perfil `clean`:

```powershell
python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean
```

### Parâmetros

| Parâmetro | Descrição |
|---|---|
| `--odate` | Data de referência da partição, no formato `YYYY-MM-DD`. |
| `--seed` | Seed utilizada para tornar a geração de dados reproduzível. |
| `--profile` | Perfil `clean` ou `chaos`. O default é `chaos` para manter compatibilidade. |

O perfil `clean` não injeta novos nulos ou duplicatas. Depois de combinar registros retidos e novos, ele aplica o contrato de colunas, normaliza os campos usados pelas regras DQ, remove registros incompletos em campos não-chave, deduplica as chaves e reconcilia as referências de atendimentos com pacientes, médicos, hospitais e doenças presentes no snapshot. No gate, essa redução é reconciliada por `removed_by_cleaning = input_rows - checked_rows`; depois da limpeza, qualquer violação restante bloqueia o salvamento da tabela inteira. O perfil `chaos` preserva as taxas de nulos e duplicatas configuradas em `config/settings.py` e é indicado para demonstrar quarentena e bloqueio fail-closed.

`clean` saneia uma base existente, mas não cria automaticamente entidades-base configuradas com zero novos registros. Em um clone sem snapshots anteriores, prepare uma fixture/seed coerente antes da primeira partição; os contratos CSV serão preservados, porém datasets-base vazios serão corretamente bloqueados pelo gate downstream.

A seed melhora a repetibilidade, mas o resultado também depende do estado de `id_control.json`, do snapshot anterior, do relógio e das versões das bibliotecas; ela não garante sozinha reprodução byte a byte.

---

## 3. Substituir CSVs já gerados

Por padrão, o gerador pode impedir a substituição de arquivos existentes. Para sobrescrever os CSVs da mesma partição, utilize a opção `--overwrite`:

```powershell
python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean --overwrite
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
python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-07-05
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

# 2. Gerar uma partição limpa para promoção até a Gold
python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean

# 3. Enviar a partição gerada para o S3
python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-07-05

# 4. Gerar e enviar eventos de streaming
python .\data-generator\main.py --streaming --stream-count 10 --seed 42 --send-eventhub
```

---

## Exemplos adicionais

### Gerar outra partição

```powershell
python .\data-generator\main.py --odate AAAA-MM-DD --seed 42 --profile clean
```

### Gerar uma partição com anomalias para testar o gate

Use uma `odate` diferente da partição limpa, pois o caminho de ingestão deve permanecer imutável:

```powershell
python .\data-generator\main.py --odate AAAA-MM-DD --seed 42 --profile chaos
```

### Gerar novamente a mesma partição

```powershell
python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean --overwrite
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
| Gerar batch aprovado | `python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean` |
| Gerar batch para testar quarentena | `python .\data-generator\main.py --odate AAAA-MM-DD --seed 42 --profile chaos` |
| Sobrescrever CSVs | `python .\data-generator\main.py --odate 2026-07-05 --seed 42 --profile clean --overwrite` |
| Gerar e enviar streaming | `python .\data-generator\main.py --streaming --stream-count 10 --seed 42 --send-eventhub` |
| Enviar partição ao S3 | `python .\data-generator\ingestion-s3\upload_to_s3.py --odate 2026-07-05` |

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
