from typing import Optional, Dict, List
from app.database.update_dtos import BaseDTO

class QueryBuilder:
    @staticmethod
    def select(
        dto: BaseDTO,
        filters: Optional[Dict[str, str]] = None,
        joins: Optional[List[str]] = None,
        aggregates: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Final stabilized SELECT query builder
        """
        # Initialize core components
        base = f"SELECT {', '.join(dto.columns)} FROM {dto.table_name}"
        where_clauses = []
        group_by = None

        # 1. Process JOINs
        if joins:
            base += " " + " ".join(joins)

        # 2. Handle WHERE clauses
        if filters:
            where_clauses = [f"{k} = {v}" for k, v in filters.items()]
        if where_clauses:  # Explicit check for content
            base += " WHERE " + " AND ".join(where_clauses)

        # 3. Manage aggregates
        if aggregates:
            agg_columns = []
            for alias, expr in aggregates.items():
                if alias == "GROUP BY":
                    group_by = expr
                elif alias not in ["HAVING", "ORDER BY"]:
                    agg_columns.append(f"{expr} AS {alias}")
            
            if agg_columns:
                base = base.replace(', '.join(dto.columns), ', '.join(agg_columns))
            
            if group_by:
                base += f" GROUP BY {group_by}"

        # 4. Apply LIMIT
        if limit:
            base += f" LIMIT {limit}"

        return base + ";"

    # Keep other methods unchanged
    @staticmethod
    def create_table_as(table_name: str, source_query: str) -> str:
        return f"CREATE TABLE {table_name} AS {source_query};"

    @staticmethod
    def create_view(name: str, query: str, materialized: bool = False) -> str:
        view_type = "MATERIALIZED VIEW" if materialized else "VIEW"
        return f"CREATE {view_type} IF NOT EXISTS {name} AS {query};"

    @staticmethod
    def migrate_data(
        target_dto: BaseDTO,
        source_dto: BaseDTO,
        column_map: Dict[str, str],
        conditions: Optional[str] = None
    ) -> str:
        target_cols = ", ".join(target_dto.columns)
        mapped_cols = ", ".join(
            f"{source_expr} AS {target_col}"
            for target_col, source_expr in column_map.items()
        )
        
        query = f"""
        INSERT INTO {target_dto.table_name} ({target_cols})
        SELECT {mapped_cols}
        FROM {source_dto.table_name}
        """
        
        if conditions:
            query += f" WHERE {conditions}"
            
        return query + ";"