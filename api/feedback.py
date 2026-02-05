from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
from api.db import get_db_connection
import threading
from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor

router = APIRouter()

class FeedbackSubmit(BaseModel):
    report_type: str = Field(..., description="Type of issue: 'missing', 'mismatch', 'outdated', 'other'")
    description: str = Field(..., description="User provided details")
    related_entities: List[Dict[str, Any]] = Field(default=[], description="List of related entities (properties, businesses, networks)")

@router.post("/api/feedback")
def submit_feedback(feedback: FeedbackSubmit, conn=Depends(get_db_connection)):
    """
    Submit user feedback about data quality issues.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO user_feedback (report_type, description, related_entities, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING id
            """, (feedback.report_type, feedback.description, json.dumps(feedback.related_entities)))
            conn.commit()
            new_id = cursor.fetchone()[0]
            
        return {"id": new_id, "status": "submitted"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/feedback")
def get_feedback(limit: int = 50, conn=Depends(get_db_connection)):
    """
    Get list of user feedback reports.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, report_type, description, related_entities, created_at 
                FROM user_feedback 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (limit,))
            feedbacks = cursor.fetchall()
            return feedbacks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
