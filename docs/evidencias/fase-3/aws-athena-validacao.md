# Evidência de validação AWS — Fase 3

Validação executada em 30/08/2026, na região `sa-east-1`, usando o workgroup
`fiap_tc_alfabetizacao_wg` e o bucket privado temporário do projeto.

## Consultas de aceite

| Validação | Query execution ID | Resultado |
|---|---|---|
| Reparação da partição UF | `e81b7da9-597e-415f-bf61-fe47cc59569b` | Concluída |
| Partições UF por ano | `7116e279-c329-4499-99ad-b97d7dd93191` | 2023: 27; 2024: 27 |
| Contagem das tabelas Gold | `fd9f670a-3557-4ff1-91e6-d2a0ed5b8430` | município: 10.704; UF: 54; Brasil: 2 |
| Indicadores Brasil | `8b4e410c-9b24-468d-b86e-f5ad5e27881a` | 2023: 55,9%; 2024: 59,2%; meta 2024: 59,9%; gap: −0,7 p.p.; variação: +3,3 p.p. |

O status nacional foi `SEM_META` em 2023 e `ABAIXO` em 2024. Os resultados
foram consultados sobre Parquet particionado por `ano=YYYY`, sem publicação de
dados individuais de alunos.

## Limpeza

Após salvar esta evidência, seguir o roteiro em
[`docs/aws-athena-temporario.md`](../../aws-athena-temporario.md): remover o
schema, excluir o workgroup e esvaziar/excluir o bucket temporário.
