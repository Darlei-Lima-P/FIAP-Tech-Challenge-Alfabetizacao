# Aplicação de inteligência artificial

## Apoio à construção

A inteligência artificial apoiou a interpretação dos requisitos, a organização
do plano de execução e a revisão de código, testes, consultas e documentação.
As decisões arquiteturais e a validação dos resultados permaneceram sob
responsabilidade da equipe.

## Uso na operação diária

O serviço proposto para IA generativa é o **Amazon Bedrock**. Ele pode consumir
o contexto agregado da camada Gold e apoiar dois fluxos:

1. **Assistente analítico:** respostas em linguagem natural, resumos de metas e
   comparação entre municípios, UFs e Brasil.
2. **Monitoria inteligente:** métricas do CloudWatch e relatórios de qualidade
   são encaminhados por EventBridge para uma função de tratamento. O Bedrock
   resume o alerta, classifica a severidade e sugere verificações. O Amazon SNS
   distribui a notificação para a equipe.

O Bedrock não altera dados nem executa correções automaticamente. Alertas e
recomendações exigem validação humana e acesso controlado por IAM.

## Modelos preditivos

Para predição tabular, a camada Gold pode alimentar modelos no **Amazon
SageMaker**, por exemplo:

- probabilidade de um município não atingir sua meta;
- previsão de evolução do indicador;
- identificação de grupos com maior vulnerabilidade educacional.

O Bedrock atua na interação, explicação e síntese; o SageMaker é responsável
pelo treinamento e disponibilização dos modelos preditivos.

## Privacidade e governança

As aplicações de IA devem consumir preferencialmente dados Gold agregados. Não
devem enviar microdados identificáveis para modelos generativos. Logs,
permissões, prompts e respostas devem seguir políticas de retenção, auditoria e
menor privilégio.
