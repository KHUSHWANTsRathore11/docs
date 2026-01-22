# Lead Generation Platform - Solution Design Requirements

## Project Overview
**Name:** Lead Generation Platform
**Target Audience:** Sales Team
**Type:** Web Application

## Key Functional Requirements
**Lead Capture:** Manual entry and external sources.
**Web Scraping:** Custom scraping capability using BeautifulSoup.
**Web Search:** Integration with **Apify** for broader web searching.
**Gen AI Features:** Integration with **Google Gemini** for lead enrichment / content generation.

## Non-Functional Requirements
*(To be populated)*

## Technology Stack
**Frontend:** React + Vite (Managed by an external team/intern)
**Backend:** Python (Evaluating Frameworks: FastAPI, Django, Flask)
**Database:** Azure SQL or PostgreSQL (Managed Service)

## Goal
Prepare prospective architecture diagrams and flows with multiple scenarios (Frameworks + Deployment).


## Integrations
**Scraping/Search:** Apify
**AI:** Google Gemini (via **AI Gateway**)
**Authentication:** **Microsoft Entra ID** (OIDC for AuthN, App Roles/Groups for AuthZ)

## Infrastructure / Deployment
**Frontend Hosting:** Azure Static Web Apps (Recommended **Standard Tier** for proper Corporate Tenant restriction; Free Tier logic is limited).
**Backend:** *(As per selected scenario)*
