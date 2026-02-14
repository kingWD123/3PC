"""
Implémentation CORRIGÉE du protocole Three-Phase Commit (3PC)
avec support pour bases de données SQL locales

CORRECTIONS:
- Utilisation de WAL mode pour SQLite (permet lectures concurrentes)
- Logs écrits APRÈS la transaction, pas pendant
- Gestion correcte des connexions fermées
"""

import sqlite3
import enum
import time
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from threading import Lock
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class TransactionState(enum.Enum):
    """États possibles d'une transaction 3PC"""
    INIT = "INIT"
    CAN_COMMIT = "CAN_COMMIT"
    PRE_COMMIT = "PRE_COMMIT"
    COMMIT = "COMMIT"
    ABORT = "ABORT"

class Vote(enum.Enum):
    """Votes possibles des participants"""
    YES = "YES"
    NO = "NO"
    ACK = "ACK"

@dataclass
class Transaction:
    """Représente une transaction distribuée"""
    transaction_id: str
    sql_queries: List[str]
    state: TransactionState = TransactionState.INIT
    timestamp: float = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

class Participant:
    """Participant dans le protocole 3PC"""

    def __init__(self, participant_id: str, db_path: str):
        self.participant_id = participant_id
        self.db_path = db_path
        self.logger = logging.getLogger(f"Participant-{participant_id}")
        self.state = TransactionState.INIT
        self.transaction_log: Dict[str, TransactionState] = {}
        self.lock = Lock()

        # Initialiser la base de données
        self._init_database()

    def _init_database(self):
        """Initialise la base de données et crée les tables de log"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # IMPORTANT: Activer le mode WAL pour permettre lectures concurrentes
        cursor.execute("PRAGMA journal_mode=WAL")

        # Table de log pour les transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transaction_log (
                transaction_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                timestamp REAL NOT NULL,
                queries TEXT
            )
        """)

        conn.commit()
        conn.close()
        self.logger.info(f"Base de données initialisée: {self.db_path}")

    def _log_transaction(self, transaction: Transaction):
        """
        Enregistre l'état de la transaction dans le log
        Cette méthode utilise une connexion SÉPARÉE
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO transaction_log 
                (transaction_id, state, timestamp, queries)
                VALUES (?, ?, ?, ?)
            """, (
                transaction.transaction_id,
                transaction.state.value,
                transaction.timestamp,
                json.dumps(transaction.sql_queries)
            ))

            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Erreur lors du logging: {e}")

    def can_commit(self, transaction: Transaction) -> Vote:
        """
        Phase 1: CAN-COMMIT
        Vérifie si la transaction peut être exécutée
        """
        with self.lock:
            try:
                self.logger.info(f"Phase 1 - CAN_COMMIT pour transaction {transaction.transaction_id}")

                # Vérifier les requêtes SQL
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                cursor = conn.cursor()

                # Tester les requêtes sans les exécuter
                for query in transaction.sql_queries:
                    # Validation basique
                    if not query.strip():
                        conn.close()
                        return Vote.NO

                conn.close()

                # Mettre à jour l'état
                self.state = TransactionState.CAN_COMMIT
                transaction.state = TransactionState.CAN_COMMIT

                # Logger APRÈS avoir fermé la connexion
                self._log_transaction(transaction)

                self.logger.info(f"Vote YES pour transaction {transaction.transaction_id}")
                return Vote.YES

            except Exception as e:
                self.logger.error(f"Erreur lors de CAN_COMMIT: {e}")
                self.state = TransactionState.ABORT
                return Vote.NO

    def pre_commit(self, transaction: Transaction) -> Vote:
        """
        Phase 2: PRE-COMMIT
        Prépare la transaction pour le commit final
        """
        with self.lock:
            try:
                self.logger.info(f"Phase 2 - PRE_COMMIT pour transaction {transaction.transaction_id}")

                if self.state != TransactionState.CAN_COMMIT:
                    self.logger.warning(f"État incorrect pour PRE_COMMIT: {self.state}")
                    return Vote.NO

                # CORRECTION: Utiliser une seule connexion pour la transaction
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                # Désactiver l'autocommit
                conn.isolation_level = None
                cursor = conn.cursor()

                # Commencer une transaction IMMEDIATE (verrouille la base)
                cursor.execute("BEGIN IMMEDIATE")

                # Exécuter les requêtes
                for query in transaction.sql_queries:
                    cursor.execute(query)

                # NE PAS COMMITER - la transaction reste ouverte
                # Sauvegarder la connexion pour la phase suivante
                self._pending_connection = conn

                # Mettre à jour l'état (en mémoire seulement)
                self.state = TransactionState.PRE_COMMIT
                transaction.state = TransactionState.PRE_COMMIT

                # IMPORTANT: Ne PAS logger ici pour éviter les conflits de verrous
                # Le log sera écrit après le commit final

                self.logger.info(f"ACK pour PRE_COMMIT transaction {transaction.transaction_id}")
                return Vote.ACK

            except Exception as e:
                self.logger.error(f"Erreur lors de PRE_COMMIT: {e}")
                # Nettoyer la connexion en cas d'erreur
                if 'conn' in locals() and conn is not None:
                    try:
                        conn.rollback()
                        conn.close()
                    except:
                        pass
                self.state = TransactionState.ABORT
                return Vote.NO

    def do_commit(self, transaction: Transaction):
        """
        Phase 3: DO-COMMIT
        Commit final de la transaction
        """
        with self.lock:
            try:
                self.logger.info(f"Phase 3 - DO_COMMIT pour transaction {transaction.transaction_id}")

                if self.state != TransactionState.PRE_COMMIT:
                    self.logger.error(f"État incorrect pour DO_COMMIT: {self.state}")
                    return

                # Commiter la transaction préparée
                if hasattr(self, '_pending_connection'):
                    self._pending_connection.commit()
                    self._pending_connection.close()
                    delattr(self, '_pending_connection')

                # Mettre à jour l'état
                self.state = TransactionState.COMMIT
                transaction.state = TransactionState.COMMIT

                # MAINTENANT on peut logger (connexion fermée, pas de conflit)
                self._log_transaction(transaction)

                self.logger.info(f"COMMIT réussi pour transaction {transaction.transaction_id}")

            except Exception as e:
                self.logger.error(f"Erreur lors de DO_COMMIT: {e}")
                self.abort(transaction)

    def abort(self, transaction: Transaction):
        """Annule la transaction"""
        with self.lock:
            try:
                self.logger.info(f"ABORT transaction {transaction.transaction_id}")

                # Rollback si une connexion est en attente
                if hasattr(self, '_pending_connection'):
                    try:
                        self._pending_connection.rollback()
                        self._pending_connection.close()
                    except Exception as e:
                        self.logger.warning(f"Erreur lors du rollback (ignorée): {e}")
                    finally:
                        delattr(self, '_pending_connection')

                # Mettre à jour l'état
                self.state = TransactionState.ABORT
                transaction.state = TransactionState.ABORT

                # Logger l'abort
                self._log_transaction(transaction)

                self.logger.info(f"Transaction {transaction.transaction_id} annulée")

            except Exception as e:
                self.logger.error(f"Erreur lors de ABORT: {e}")

class Coordinator:
    """Coordinateur du protocole 3PC"""

    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self.logger = logging.getLogger(f"Coordinator-{coordinator_id}")
        self.participants: List[Participant] = []
        self.lock = Lock()

    def add_participant(self, participant: Participant):
        """Ajoute un participant au protocole"""
        with self.lock:
            self.participants.append(participant)
            self.logger.info(f"Participant {participant.participant_id} ajouté")

    def execute_transaction(self, transaction: Transaction) -> bool:
        """
        Exécute une transaction distribuée avec le protocole 3PC
        Retourne True si la transaction a réussi, False sinon
        """
        self.logger.info(f"Début de la transaction {transaction.transaction_id}")

        try:
            # Phase 1: CAN-COMMIT
            self.logger.info("=== PHASE 1: CAN-COMMIT ===")
            votes = []
            for participant in self.participants:
                vote = participant.can_commit(transaction)
                votes.append(vote)
                self.logger.info(f"Participant {participant.participant_id} vote: {vote.value}")

            # Si au moins un participant vote NO, ABORT
            if Vote.NO in votes:
                self.logger.warning("Au moins un participant a voté NO - ABORT")
                self._abort_all(transaction)
                return False

            self.logger.info("Tous les participants ont voté YES")

            # Phase 2: PRE-COMMIT
            self.logger.info("=== PHASE 2: PRE-COMMIT ===")
            acks = []
            for participant in self.participants:
                ack = participant.pre_commit(transaction)
                acks.append(ack)
                self.logger.info(f"Participant {participant.participant_id} ACK: {ack.value}")

            # Si au moins un participant n'ACK pas, ABORT
            if Vote.ACK not in acks or Vote.NO in acks:
                self.logger.warning("Problème lors de PRE-COMMIT - ABORT")
                self._abort_all(transaction)
                return False

            self.logger.info("Tous les participants sont en PRE-COMMIT")

            # Phase 3: DO-COMMIT
            self.logger.info("=== PHASE 3: DO-COMMIT ===")
            for participant in self.participants:
                participant.do_commit(transaction)
                self.logger.info(f"Participant {participant.participant_id} a commité")

            self.logger.info(f"Transaction {transaction.transaction_id} RÉUSSIE")
            return True

        except Exception as e:
            self.logger.error(f"Erreur lors de l'exécution de la transaction: {e}")
            self._abort_all(transaction)
            return False

    def _abort_all(self, transaction: Transaction):
        """Annule la transaction sur tous les participants"""
        self.logger.info("Annulation de la transaction sur tous les participants")
        for participant in self.participants:
            try:
                participant.abort(transaction)
            except Exception as e:
                self.logger.error(f"Erreur lors de l'abort du participant {participant.participant_id}: {e}")