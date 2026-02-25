"""
Firebase Admin SDK Configuration for AATB
Architectural Choice: Firebase provides zero-ops, real-time syncing with built-in offline persistence.
Critical for the Control Layer's state management and event streaming.
"""
import os
import logging
from typing import Optional, Dict, Any
import json
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore, db
from google.cloud.firestore_v1 import Client as FirestoreClient
from google.cloud.firestore_v1.base_document import DocumentSnapshot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FirebaseManager:
    """Singleton manager for Firebase services with proper error handling"""
    
    _instance: Optional['FirebaseManager'] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.app: Optional[firebase_admin.App] = None
            self.firestore_client: Optional[FirestoreClient] = None
            self.realtime_db: Optional[db.Reference] = None
            self._initialized = True
    
    def initialize(self, credential_path: Optional[str] = None) -> None:
        """
        Initialize Firebase Admin SDK with multiple fallback strategies
        
        Edge Cases Handled:
        1. Missing credential file
        2. Invalid credential JSON
        3. Already initialized app
        4. Environment variable credentials (for Cloud Functions)
        """
        if self.app is not None:
            logger.warning("Firebase app already initialized")
            return
        
        try:
            # Strategy 1: Explicit credential file
            if credential_path and os.path.exists(credential_path):
                cred = credentials.Certificate(credential_path)
                logger.info(f"Initializing Firebase with credential file: {credential_path}")
            
            # Strategy 2: GOOGLE_APPLICATION_CREDENTIALS environment variable
            elif os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                cred = credentials.ApplicationDefault()
                logger.info("Initializing Firebase with environment credentials")
            
            # Strategy 3: Service account JSON in environment variable
            elif os.environ.get('FIREBASE_SERVICE_ACCOUNT'):
                service_account_info = json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT'])
                cred = credentials.Certificate(service_account_info)
                logger.info("Initializing Firebase with environment JSON")
            
            # Strategy 4: Default initialization (for Cloud Functions/App Engine)
            else:
                cred = credentials.ApplicationDefault()
                logger.warning("Initializing Firebase with default credentials - ensure proper IAM roles")
            
            self.app = firebase_admin.initialize_app(cred, {
                'projectId': os.environ.get('FIREBASE_PROJECT_ID', 'aatb-production'),
                'databaseURL': os.environ.get('FIREBASE_DATABASE_URL', '')
            })
            
            # Initialize services
            self.firestore_client = firestore.client(self.app)
            
            if os.environ.get('FIREBASE_DATABASE_URL'):
                self.realtime_db = db.reference('/', app=self.app)
                logger.info("Realtime Database initialized")
            
            logger.info("Firebase initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {str(e)}")
            raise RuntimeError(f"Firebase initialization failed: {str(e)}")
    
    def get_firestore(self) -> FirestoreClient:
        """Get Firestore client with validation"""
        if self.firestore_client is None:
            self.initialize()
            if self.firestore_client is None:
                raise RuntimeError("Firestore client not available after initialization")
        return self.firestore_client
    
    def get_realtime_db(self) -> Optional[db.Reference]:
        """Get Realtime Database reference"""
        if self.realtime_db is None and os.environ.get('FIREBASE_DATABASE_URL'):
            self.initialize()
        return self.realtime_db
    
    def update_market_state(self, 
                           symbol: str, 
                           state_data: Dict[str, Any], 
                           collection: str = "market_states") -> None:
        """
        Update market state in Firestore with timestamp and validation
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            state_data: Dictionary containing market state
            collection: Firestore collection name
        """
        try:
            if not symbol or not isinstance(state_data, dict):
                raise ValueError("Invalid symbol or state_data")
            
            # Add metadata
            enriched_data = {
                **state_data,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'symbol': symbol,
                'updated_at': datetime.utcnow().isoformat(),
                'source': 'aatb_intelligence_layer'
            }
            
            doc_ref = self.get_firestore().collection(collection).document(symbol)
            doc_ref.set(enriched_data, merge=True)
            
            logger.debug(f"Updated market state for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to update market state for {symbol}: {str(e)}")
            # Critical: Don't fail silently for state updates
            raise
    
    def stream_market_updates(self, 
                             symbol: str,
                             callback,
                             collection: str = "market_states") -> None:
        """
        Stream real-time market updates from Firestore
        
        Args:
            symbol: Trading pair to stream
            callback: Function to call on document updates
            collection: Firestore collection to watch
        """
        try:
            doc_ref = self.get_firestore().collection(collection).document(symbol)
            
            def on_snapshot(doc_snapshot: DocumentSnapshot, changes, read_time):
                if doc_snapshot.exists:
                    callback(doc_snapshot.to_dict