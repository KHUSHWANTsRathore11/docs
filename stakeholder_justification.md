# Architecture Decision: Handling Long-Running & Cyclical Workloads

## The Challenge
We have identified two critical characteristics of the backend workload:
1.  **Extreme Duration:** Scraping and GenAI enrichment tasks may run for **hours or up to a day**.
2.  **Cyclical Usage:** High demand during specific financial cycles, with low/zero usage on weekends.

## Comparative Analysis

| Feature | Scenario 1: Container Apps (Recommended) | Scenario 2: Django Monolith (App Service) | Scenario 3: Serverless (Azure Functions) |
| :--- | :--- | :--- | :--- |
| **Max Execution Time** | **Unlimited** (via Jobs) | Unlimited (Technically) | **5-10 Minutes** (Hard Limit) |
| **Reliability** | **High** (Job runs in isolation) | **Medium/Low** (Deployments restart the server) | **N/A** (Cannot finish the task) |
| **Cost Efficiency** | **Excellent** (Scales to zero when idle) | **Poor** (Must pay for peak capacity even on weekends) | **Good** (But fails the duration req) |

## Why We Recommned Scenario 1 (Container Apps)

### 1. Handling "Hours-Long" Tasks
*   **The Problem with Serverless:** Azure Functions has a hard timeout (usually 10 mins). It will simply kill our process in the middle of a 2-hour scrape.
*   **The Problem with PaaS (App Service):** If we deploy a bug fix or security patch to the main website, the entire server restarts. Any background jobs running at that moment are killed instantly.
*   **The Solution (Container Jobs):** Azure Container Apps separates the "Website" from the "Worker". We can run the worker as a **"Job"**. A Job is designed to start, run for 24 hours if needed, and finish. If we update the website code, **the running job is untouched** and continues processing.

### 2. Handling Financial Cycles (Bursty Cost)
*   **The Problem with PaaS:** To handle the heavy "Financial Cycle" load, we must provision a large, expensive server. On weekends, when usage is zero, **we still pay for that large server** unless we manually downgrade it every Friday.
*   **The Solution (KEDA Scaling):** Container Apps uses "Event-Driven Scaling" (KEDA). 
    *   **Weekends:** The queue is empty. Azure spins down to **0 containers**. Cost = **$0**.
    *   **Financial Cycle:** You dump 10,000 leads. Azure sees the queue spike and automatically spins up **50 parallel containers**. You pay only for the compute used to crunch that data, then it drops back to zero.

## Executive Summary
**Scenario 1 (Container Apps) is the only option** that offers the technical capability to run day-long tasks reliably while maintaining the "pay-for-what-you-use" economic model required for our cynical workload. Scenario 3 fails on capability (timeouts), and Scenario 2 fails on cost efficiency (idle payments).
