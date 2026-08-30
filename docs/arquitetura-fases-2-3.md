# Arquitetura completa da solução

A solução integra ingestão batch e streaming, armazenamento em arquitetura
medalhão, processamento de qualidade, consultas analíticas, observabilidade e
aplicações de inteligência artificial.

```mermaid
flowchart LR
    A[Base dos Dados e fontes educacionais] --> B[Ingestão batch]
    A --> C[Streaming simulado]
    B --> D[Amazon S3 - Bronze]
    C --> D
    D --> E[Processamento e qualidade]
    E --> F[Amazon S3 - Silver]
    F --> G[Indicadores e agregações]
    G --> H[Amazon S3 - Gold]
    H --> I[AWS Glue Data Catalog]
    I --> J[Amazon Athena]
    J --> K[Consultas e visualizações]
    H --> L[Amazon Bedrock e SageMaker]
    M[CloudWatch, EventBridge e SNS] -. monitora .-> B
    M -. monitora .-> E
    M -. monitora .-> G
    M --> L
```

O diagrama visual em alta resolução está em
[`docs/arquitetura-completa.png`](arquitetura-completa.png).
