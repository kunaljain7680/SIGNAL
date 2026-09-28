"""seed curated technologies

Revision ID: 96818d0fc0da
Revises: 0dc3ee276aee
Create Date: 2026-09-29 01:54:50.025756

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '96818d0fc0da'
down_revision: Union[str, None] = '0dc3ee276aee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Define a lightweight representation of the table for the migration
    technologies_table = sa.table(
        'technologies',
        sa.column('canonical_name', sa.Text),
        sa.column('slug', sa.Text),
        sa.column('description', sa.Text),
        sa.column('domain_tags', postgresql.ARRAY(sa.Text)),
        sa.column('learning_effort_hours_estimate', sa.Integer),
        sa.column('status', sa.Text)
    )

    # 2. Curated seed data for exactly 28 technologies
    seed_data = [
        # Backend
        {"canonical_name": "Python", "slug": "python", "description": "High-level, general-purpose programming language.", "domain_tags": ["backend", "data", "ai"], "learning_effort_hours_estimate": 100, "status": "active"},
        {"canonical_name": "FastAPI", "slug": "fastapi", "description": "Modern, fast, highly performant web framework for building APIs with Python.", "domain_tags": ["backend", "framework"], "learning_effort_hours_estimate": 40, "status": "active"},
        {"canonical_name": "Django", "slug": "django", "description": "High-level Python web framework that encourages rapid development and clean, pragmatic design.", "domain_tags": ["backend", "framework"], "learning_effort_hours_estimate": 70, "status": "active"},
        {"canonical_name": "Flask", "slug": "flask", "description": "Lightweight WSGI web application framework for Python.", "domain_tags": ["backend", "framework"], "learning_effort_hours_estimate": 30, "status": "active"},
        {"canonical_name": "Node.js", "slug": "node-js", "description": "Asynchronous event-driven JavaScript runtime designed to build scalable network applications.", "domain_tags": ["backend", "javascript"], "learning_effort_hours_estimate": 50, "status": "active"},
        {"canonical_name": "Go", "slug": "go", "description": "Statically typed, compiled programming language designed at Google.", "domain_tags": ["backend", "systems"], "learning_effort_hours_estimate": 60, "status": "active"},
        {"canonical_name": "Rust", "slug": "rust", "description": "Multi-paradigm, general-purpose programming language that emphasizes performance, type safety, and concurrency.", "domain_tags": ["backend", "systems"], "learning_effort_hours_estimate": 120, "status": "active"},
        {"canonical_name": "Java", "slug": "java", "description": "High-level, class-based, object-oriented programming language.", "domain_tags": ["backend", "enterprise"], "learning_effort_hours_estimate": 100, "status": "active"},
        {"canonical_name": "Spring Boot", "slug": "spring-boot", "description": "Open-source Java-based framework used to create microservices.", "domain_tags": ["backend", "framework"], "learning_effort_hours_estimate": 80, "status": "active"},
        
        # Databases / Data
        {"canonical_name": "PostgreSQL", "slug": "postgresql", "description": "Powerful, open source object-relational database system.", "domain_tags": ["database", "sql"], "learning_effort_hours_estimate": 50, "status": "active"},
        {"canonical_name": "Redis", "slug": "redis", "description": "In-memory data structure store, used as a distributed, in-memory key–value database, cache and message broker.", "domain_tags": ["database", "cache"], "learning_effort_hours_estimate": 25, "status": "active"},
        {"canonical_name": "MongoDB", "slug": "mongodb", "description": "Source-available cross-platform document-oriented database program.", "domain_tags": ["database", "nosql"], "learning_effort_hours_estimate": 40, "status": "active"},
        {"canonical_name": "Elasticsearch", "slug": "elasticsearch", "description": "Distributed, RESTful search and analytics engine.", "domain_tags": ["database", "search"], "learning_effort_hours_estimate": 70, "status": "active"},
        
        # Cloud / Platform
        {"canonical_name": "Docker", "slug": "docker", "description": "Platform as a service products that use OS-level virtualization to deliver software in packages called containers.", "domain_tags": ["cloud", "devops"], "learning_effort_hours_estimate": 40, "status": "active"},
        {"canonical_name": "Kubernetes", "slug": "kubernetes", "description": "Open-source container orchestration system for automating software deployment, scaling, and management.", "domain_tags": ["cloud", "devops"], "learning_effort_hours_estimate": 100, "status": "active"},
        {"canonical_name": "Azure", "slug": "azure", "description": "Cloud computing platform operated by Microsoft.", "domain_tags": ["cloud", "platform"], "learning_effort_hours_estimate": 80, "status": "active"},
        {"canonical_name": "AWS", "slug": "aws", "description": "Comprehensive, evolving cloud computing platform provided by Amazon.", "domain_tags": ["cloud", "platform"], "learning_effort_hours_estimate": 80, "status": "active"},
        {"canonical_name": "Terraform", "slug": "terraform", "description": "Infrastructure as code software tool that provides a consistent CLI workflow to manage hundreds of cloud services.", "domain_tags": ["cloud", "iac"], "learning_effort_hours_estimate": 60, "status": "active"},
        
        # AI / LLM / AI Infrastructure
        {"canonical_name": "PyTorch", "slug": "pytorch", "description": "Machine learning framework based on the Torch library.", "domain_tags": ["ai", "machine-learning"], "learning_effort_hours_estimate": 80, "status": "active"},
        {"canonical_name": "Hugging Face", "slug": "hugging-face", "description": "Company and hub providing tools and models for machine learning and NLP.", "domain_tags": ["ai", "nlp"], "learning_effort_hours_estimate": 30, "status": "active"},
        {"canonical_name": "LangChain", "slug": "langchain", "description": "Framework designed to simplify the creation of applications using large language models.", "domain_tags": ["ai", "llm"], "learning_effort_hours_estimate": 40, "status": "active"},
        {"canonical_name": "LangGraph", "slug": "langgraph", "description": "Library for building stateful, multi-actor applications with LLMs.", "domain_tags": ["ai", "llm", "agents"], "learning_effort_hours_estimate": 35, "status": "active"},
        {"canonical_name": "vLLM", "slug": "vllm", "description": "High-throughput and memory-efficient LLM inference and serving engine.", "domain_tags": ["ai", "infrastructure"], "learning_effort_hours_estimate": 45, "status": "active"},
        {"canonical_name": "MCP", "slug": "mcp", "description": "Model Context Protocol for standardizing communication between LLMs and external systems.", "domain_tags": ["ai", "infrastructure"], "learning_effort_hours_estimate": 25, "status": "active"},
        
        # Distributed / Messaging
        {"canonical_name": "Apache Kafka", "slug": "apache-kafka", "description": "Distributed event store and stream-processing platform.", "domain_tags": ["distributed", "messaging"], "learning_effort_hours_estimate": 90, "status": "active"},
        {"canonical_name": "RabbitMQ", "slug": "rabbitmq", "description": "Open-source message-broker software that originally implemented the Advanced Message Queuing Protocol.", "domain_tags": ["distributed", "messaging"], "learning_effort_hours_estimate": 45, "status": "active"},
        
        # Frontend
        {"canonical_name": "React", "slug": "react", "description": "Free and open-source front-end JavaScript library for building user interfaces based on components.", "domain_tags": ["frontend", "javascript"], "learning_effort_hours_estimate": 60, "status": "active"},
        {"canonical_name": "TypeScript", "slug": "typescript", "description": "Strict syntactical superset of JavaScript and adds optional static typing to the language.", "domain_tags": ["frontend", "language"], "learning_effort_hours_estimate": 40, "status": "active"}
    ]

    # 3. Use PostgreSQL UPSERT to insert new rows and overwrite existing dev rows (FastAPI/Rust)
    stmt = postgresql.insert(technologies_table).values(seed_data)
    
    stmt = stmt.on_conflict_do_update(
        index_elements=['canonical_name'],
        set_={
            'slug': stmt.excluded.slug,
            'description': stmt.excluded.description,
            'domain_tags': stmt.excluded.domain_tags,
            'learning_effort_hours_estimate': stmt.excluded.learning_effort_hours_estimate,
            'status': stmt.excluded.status
        }
    )

    op.execute(stmt)


def downgrade() -> None:
    # Explicit no-op to prevent accidentally wiping pre-existing user data or other catalogs
    pass