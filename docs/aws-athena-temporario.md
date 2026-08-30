# Execução temporária no Amazon S3 e Athena

Este roteiro cria somente os recursos necessários para comprovar as consultas da camada Gold. Não utiliza chaves de acesso locais e não exige AWS CLI.

## 1. Recursos mínimos

- Região: `sa-east-1` — São Paulo.
- Bucket privado: `fiap-tc-alfabetizacao-<sufixo-unico>-sa-east-1`.
- Banco do catálogo: `fiap_tc_alfabetizacao`.
- Workgroup: `fiap_tc_alfabetizacao_wg`.

No bucket, manter o bloqueio total de acesso público, criptografia padrão SSE-S3, propriedade `Bucket owner enforced` e versionamento desativado. Criar os prefixos:

```text
gold/
athena-results/
```

Não é necessário configurar alertas, QuickSight, Glue Crawler, Glue Job, Lambda, KMS própria ou capacidade provisionada.

## 2. Publicação dos arquivos

Enviar pelo Console do S3 as três pastas locais, preservando os diretórios `ano=YYYY`:

```text
data/gold/indicadores_municipio/  -> gold/indicadores_municipio/
data/gold/indicadores_uf/         -> gold/indicadores_uf/
data/gold/indicadores_brasil/     -> gold/indicadores_brasil/
```

O upload da Bronze, da Silver detalhada e dos microdados de alunos não é necessário para a prova analítica.

## 3. Configuração do Athena

Criar o workgroup `fiap_tc_alfabetizacao_wg` com:

- Athena engine version 3;
- resultado das consultas em `s3://<bucket>/athena-results/`;
- configurações do workgroup substituindo as configurações do cliente;
- criptografia SSE-S3;
- limite de 256 MB lidos por consulta.

No Query Editor, selecionar esse workgroup. Abrir `sql/athena/create_gold_tables.sql`, substituir `SEU_BUCKET` pelo nome criado e executar as instruções. Em seguida, executar `sql/athena/consultas_gold.sql`.

Valores de aceite esperados:

| Tabela | Registros |
|---|---:|
| `gold_indicadores_municipio` | 10.704 |
| `gold_indicadores_uf` | 54 |
| `gold_indicadores_brasil` | 2 |

No Brasil, o resultado esperado para 2024 é taxa de 59,2%, meta de 59,9% e variação de +3,3 pontos percentuais em relação a 2023.

## 4. Evidências

Registrar apenas informações acadêmicas, ocultando e-mail e ID da conta:

- objetos Gold no bucket privado;
- workgroup selecionado;
- tabelas encontradas pelo `SHOW TABLES`;
- status `Succeeded`, Query execution ID, tempo e dados lidos;
- resultado das consultas de Brasil, UFs, metas e evolução.

As imagens podem ser armazenadas em `docs/evidencias/aws/`. Não publicar dados individuais de alunos.

## 5. Limpeza após a validação

Depois de salvar as evidências:

1. Executar `DROP DATABASE IF EXISTS fiap_tc_alfabetizacao CASCADE;` no Athena.
2. Excluir o workgroup `fiap_tc_alfabetizacao_wg`.
3. Esvaziar completamente e excluir o bucket temporário.
4. Confirmar que o banco, o workgroup e o bucket não existem mais em `sa-east-1`.
5. Conferir a área de faturamento no dia seguinte e no fechamento do mês.

Não existe um botão para "desligar o faturamento" do Athena. Sem capacidade provisionada, o Athena não cobra por ficar parado; a cobrança ocorre pelas consultas executadas e pelo armazenamento dos resultados no S3. A remoção dos recursos encerra o consumo contínuo deste projeto.
