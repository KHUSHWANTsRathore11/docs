# Database Selection: Lead Generation Platform

## Recommendation: Azure Database for PostgreSQL (Flexible Server)

We strongly recommend **PostgreSQL** over Azure SQL for this specific Python + GenAI workload.

### Why PostgreSQL?
1.  **Python Native:** PostgreSQL is the default standard for the Python/FastAPI/Django ecosystem.
2.  **Gen AI Ready:** Native `pgvector` extension allows storing and querying embeddings (Semantic Search) without a separate Vector Database.
3.  **JSONB Support:** Best-in-class support for storing unstructured scraped data (JSON) alongside structured relational data.

---

## Detailed Pricing Comparison (Estimated Pay-As-You-Go)

The following comparison highlights the cost differences between **Azure Database for PostgreSQL (Flexible Server)** and **Azure SQL Database (vCore Model)**.

*Note: Prices are estimates based on East US region, Linux (where applicable), and Pay-As-You-Go rates. Reserved Instances can reduce these by 40-60%.*

### 1. Compute Costs

| Tier | **PostgreSQL Flexible Server** | **Azure SQL Database (vCore)** | **Comparison Notes** |
| :--- | :--- | :--- | :--- |
| **Burstable / Dev** | **Tier:** Burstable (B1ms)<br>**Specs:** 1 vCore, 2 GB RAM<br>**Cost:** ~$13 / month | **Tier:** Serverless / Basic<br>**Specs:** 1 vCore eq.<br>**Cost:** ~$5 (DTU Basic) - $15 (Serverless) | Azure SQL Basic (DTU) is cheaper for strict dev, but Postgres B-Series is full vCore performance. |
| **General Purpose** | **Tier:** General Purpose (D2s_v3)<br>**Specs:** 2 vCores, 8 GB RAM<br>**Cost:** ~$130 / month | **Tier:** General Purpose (Gen5)<br>**Specs:** 2 vCores, 10 GB RAM<br>**Cost:** ~$368 / month | **PostgreSQL is significantly cheaper (~65% less)** for standard provisioned compute in this tier. |
| **Memory Optimized** | **Tier:** Memory Optimized (E2s_v3)<br>**Specs:** 2 vCores, 16 GB RAM<br>**Cost:** ~$170 / month | **Tier:** Business Critical<br>**Specs:** 2 vCores, 10.2 GB RAM (High IO)<br>**Cost:** ~$985 / month | Azure SQL "Business Critical" includes high-availability replicas, driving up cost. Postgres Memory Optimized is much more accessible. |

### 2. Storage Costs

| Resource | **PostgreSQL Flexible Server** | **Azure SQL Database** |
| :--- | :--- | :--- |
| **Premium SSD Storage** | ~$0.115 per GB / month | ~$0.115 per GB / month |
| **Backup Storage** | First 32GB Free, then ~$0.05/GB | ~$0.10/GB (PITR) |
| **IOPS** | Included (up to limit) | Provisioned IOPS can cost extra in some tiers |

---

## Conclusion

For a **Python-centric, GenAI-enabled application**, **PostgreSQL Flexible Server** provides:
1.  **Better Value:** The General Purpose and Memory Optimized tiers are 2-3x cheaper than their Azure SQL counterparts for raw compute.
2.  **Feature Fit:** Native Vector support (`pgvector`) saves the cost and complexity of a specialized Vector DB.
3.  **Flexibility:** The Burstable tier is perfect for enabling low-cost development environments for the team.
