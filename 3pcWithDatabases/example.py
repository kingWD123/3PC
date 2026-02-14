"""
Exemple Corrigé du Protocole 3PC
Résout les problèmes de verrous SQLite et de fermeture de fichiers
"""

from three_phase_commit import Coordinator, Participant, Transaction
import sqlite3
import uuid
import os
import time
import gc

def creer_bases_donnees():
    """Crée 3 bases de données bancaires identiques"""
    print(" Création des bases de données...")

    bases = []
    for i in range(1, 4):
        nom_db = f"banque_replica_{i}.db"

        # Supprimer si existe déjà (avec retry)
        if os.path.exists(nom_db):
            for attempt in range(3):
                try:
                    os.remove(nom_db)
                    break
                except PermissionError:
                    time.sleep(0.5)
                    gc.collect()  # Force garbage collection

        # Créer la base
        conn = sqlite3.connect(nom_db)
        cursor = conn.cursor()

        # Activer WAL mode (important!)
        cursor.execute("PRAGMA journal_mode=WAL")

        # Table des comptes
        cursor.execute("""
            CREATE TABLE comptes (
                id INTEGER PRIMARY KEY,
                nom TEXT NOT NULL,
                solde REAL NOT NULL
            )
        """)

        # Données initiales
        cursor.execute("INSERT INTO comptes VALUES (1, 'Alice', 1000.0)")
        cursor.execute("INSERT INTO comptes VALUES (2, 'Bob', 500.0)")
        cursor.execute("INSERT INTO comptes VALUES (3, 'Charlie', 750.0)")

        conn.commit()
        conn.close()

        bases.append(nom_db)
        print(f"    {nom_db} créée")

    return bases

def afficher_etat(bases):
    """Affiche l'état de toutes les bases de données"""
    print("\n" + "="*60)
    for i, db in enumerate(bases, 1):
        print(f"\n Base de données {i} - {db}")
        print("-" * 60)

        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM comptes ORDER BY id")

        print(f"{'ID':>4} | {'Nom':<10} | {'Solde':>10}")
        print("-" * 60)

        for row in cursor.fetchall():
            print(f"{row[0]:>4} | {row[1]:<10} | {row[2]:>10.2f}FCFA")

        conn.close()
    print("="*60)

def nettoyer_bases(bases):
    """Nettoie les bases de données avec gestion d'erreurs"""
    print("\n Nettoyage...")

    # Forcer la libération des ressources
    gc.collect()
    time.sleep(0.5)

    for db in bases:
        for attempt in range(5):
            try:
                if os.path.exists(db):
                    os.remove(db)
                    print(f"    {db} supprimée")
                # Supprimer aussi les fichiers WAL et SHM
                for ext in ['-wal', '-shm']:
                    wal_file = db + ext
                    if os.path.exists(wal_file):
                        os.remove(wal_file)
                break
            except PermissionError:
                if attempt < 4:
                    print(f"    Attente pour {db} (tentative {attempt+1}/5)...")
                    time.sleep(1)
                    gc.collect()
                else:
                    print(f"     Impossible de supprimer {db} (fichier verrouillé)")

def exemple_transfert_reussi():
    """Exemple complet d'un transfert réussi"""
    print("\n" + "*"*30)
    print("EXEMPLE: TRANSFERT BANCAIRE DISTRIBUÉ")
    print("*"*30)

    # Étape 1: Créer les bases de données
    bases = creer_bases_donnees()

    print("\n ÉTAT INITIAL")
    afficher_etat(bases)

    # Étape 2: Configurer le protocole 3PC
    print("\n  Configuration du protocole 3PC...")
    coordinateur = Coordinator("coordinateur-banque")

    participants = []
    for i, db in enumerate(bases, 1):
        participant = Participant(f"replica-{i}", db)
        coordinateur.add_participant(participant)
        participants.append(participant)
        print(f"    Participant {i} enregistré ({db})")

    # Étape 3: Créer la transaction
    print("\n CRÉATION DE LA TRANSACTION")
    print("   Opération: Alice transfère 200 FCFA à Bob")
    print("   Cette transaction sera exécutée sur les 3 bases de données")

    transaction = Transaction(
        transaction_id=str(uuid.uuid4()),
        sql_queries=[
            # Débiter Alice
            "UPDATE comptes SET solde = solde - 200.0 WHERE id = 1",
            # Créditer Bob
            "UPDATE comptes SET solde = solde + 200.0 WHERE id = 2"
        ]
    )

    print(f"   ID Transaction: {transaction.transaction_id[:16]}...")

    # Étape 4: Exécuter avec 3PC
    print("\n EXÉCUTION DU PROTOCOLE 3PC")
    print("   Phase 1: CAN-COMMIT (tous les participants votent)")
    print("   Phase 2: PRE-COMMIT (préparation)")
    print("   Phase 3: DO-COMMIT (commit final)")
    print()

    succes = coordinateur.execute_transaction(transaction)

    # IMPORTANT: Libérer les participants pour fermer les connexions
    del participants
    del coordinateur
    gc.collect()
    time.sleep(0.2)

    # Étape 5: Afficher le résultat
    print("\n ÉTAT FINAL")
    afficher_etat(bases)

    # Étape 6: Vérification
    print("\n" + "="*60)
    if succes:
        print(" SUCCÈS!")
        print("   La transaction a été appliquée sur toutes les bases")
        print("   Les 3 répliques sont maintenant synchronisées")

        # Vérifier la cohérence
        soldes_alice = []
        soldes_bob = []

        for db in bases:
            conn = sqlite3.connect(db)
            cursor = conn.cursor()
            cursor.execute("SELECT solde FROM comptes WHERE id = 1")
            soldes_alice.append(cursor.fetchone()[0])
            cursor.execute("SELECT solde FROM comptes WHERE id = 2")
            soldes_bob.append(cursor.fetchone()[0])
            conn.close()

        coherent = (len(set(soldes_alice)) == 1 and
                   len(set(soldes_bob)) == 1)

        if coherent:
            print(f"\n    COHÉRENCE VÉRIFIÉE:")
            print(f"      Alice: {soldes_alice[0]:.2f}FCFA sur toutes les répliques")
            print(f"      Bob: {soldes_bob[0]:.2f}FCFA sur toutes les répliques")
        else:
            print("\n     ATTENTION: Incohérence détectée entre les bases!")
    else:
        print(" ÉCHEC")
        print("   La transaction a été annulée (rollback)")
        print("   Toutes les bases ont conservé leur état initial")

    print("="*60)

    # Nettoyage
    nettoyer_bases(bases)

    return succes

def exemple_transaction_echouee():
    """Exemple d'une transaction qui échoue"""
    print("\n" + "+"*30)
    print("EXEMPLE: TRANSACTION INVALIDE (ROLLBACK)")
    print("+"*30)

    bases = creer_bases_donnees()

    print("\n ÉTAT INITIAL")
    afficher_etat(bases)

    print("\n  Configuration du protocole 3PC...")
    coordinateur = Coordinator("coordinateur-test")

    for i, db in enumerate(bases, 1):
        participant = Participant(f"replica-{i}", db)
        coordinateur.add_participant(participant)

    # Transaction avec une requête invalide
    print("\n CRÉATION D'UNE TRANSACTION INVALIDE")
    print("   ⚠  Cette transaction contient une erreur volontaire")

    transaction = Transaction(
        transaction_id=str(uuid.uuid4()),
        sql_queries=[
            "UPDATE comptes SET solde = solde - 300.0 WHERE id = 1",
            ""  # Requête vide - provoquera une erreur
        ]
    )

    print("\n EXÉCUTION DU PROTOCOLE 3PC")
    succes = coordinateur.execute_transaction(transaction)

    # Libérer les ressources
    del coordinateur
    gc.collect()
    time.sleep(0.2)

    print("\n📋 ÉTAT FINAL")
    afficher_etat(bases)

    print("\n" + "="*60)
    if not succes:
        print(" ROLLBACK RÉUSSI!")
        print("   La transaction invalide a été correctement rejetée")
        print("   Aucune base de données n'a été modifiée")
        print("   Toutes les données restent intactes")
    else:
        print(" PROBLÈME: Cette transaction aurait dû échouer")
    print("="*60)

    # Nettoyage
    nettoyer_bases(bases)

    return not succes

if __name__ == "__main__":
    print("\n" + "*"*30)
    print("DÉMONSTRATION DU PROTOCOLE 3PC")
    print("Transactions Distribuées sur Bases de Données SQL")
    print("*"*30)



    print("\nLe protocole 3PC garantit que:")
    print("  • Une transaction est appliquée sur TOUTES les bases")
    print("  • OU sur AUCUNE base (rollback automatique)")
    print("  • Les données restent toujours cohérentes")

    input("\n[Appuyez sur Entrée pour commencer le premier exemple...]")

    resultat1 = exemple_transfert_reussi()

    input("\n[Appuyez sur Entrée pour le deuxième exemple...]")

    resultat2 = exemple_transaction_echouee()

    # Résumé final
    print("\n" + "-"*30)
    print("RÉSUMÉ")
    print("-"*30)
    print(f"\nExemple 1 (Transfert réussi):     {'✅ RÉUSSI' if resultat1 else '❌ ÉCHOUÉ'}")
    print(f"Exemple 2 (Transaction invalide): {'✅ RÉUSSI' if resultat2 else '❌ ÉCHOUÉ'}")

    if resultat1 and resultat2:
        print("\n Tous les exemples ont fonctionné correctement!")
        print("\nVous pouvez maintenant adapter ce code pour:")
        print("  • Vos propres bases de données")
        print("  • PostgreSQL, MySQL (pas de problème de verrous)")
        print("  • Vos cas d'usage spécifiques")

    print("\n" + "="*90)