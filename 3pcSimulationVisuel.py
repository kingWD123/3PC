import tkinter as tk
from tkinter import scrolledtext
import time
import threading

# --- CONFIGURATION VISUELLE ---
COLOR_DEFAULT = "#d9d9d9"  # Gris
COLOR_WAIT = "#f1c40f"     # Jaune (Attente/Vote)
COLOR_PRE = "#3498db"      # Bleu (Pre-Commit)
COLOR_COMMIT = "#2ecc71"   # Vert (Commit Succès)
COLOR_ABORT = "#e74c3c"    # Rouge (Abort/Echec)
COLOR_COORD = "#9b59b6"    # Violet (Coordinateur)

class NodeWidget:
    """Représente un cercle (Nœud) sur le canevas"""
    def __init__(self, canvas, x, y, text, is_coord=False):
        self.canvas = canvas
        self.id = text
        r = 40  # Rayon

        # Cercle
        color = COLOR_COORD if is_coord else COLOR_DEFAULT
        self.oval = canvas.create_oval(x-r, y-r, x+r, y+r, fill=color, outline="black", width=2)

        # Label du nom
        self.label = canvas.create_text(x, y-55, text=text, font=("Arial", 10, "bold"))

        # Label de l'état
        self.status_text = canvas.create_text(x, y, text="IDLE", font=("Arial", 9))

    def set_state(self, state_text, color):
        self.canvas.itemconfig(self.oval, fill=color)
        self.canvas.itemconfig(self.status_text, text=state_text)

class SimulatorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Simulateur Visuel 3-Phase Commit (3PC)")
        self.root.geometry("800x600")

        # 1. Zone de Dessin (Canvas)
        self.canvas = tk.Canvas(root, bg="white", height=400)
        self.canvas.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Création des Nœuds
        # Coordinateur en haut
        self.coord_node = NodeWidget(self.canvas, 400, 80, "COORDINATEUR", is_coord=True)

        # Participants en bas
        self.nodes = []
        positions = [200, 400, 600]
        for i, pos in enumerate(positions):
            n = NodeWidget(self.canvas, pos, 300, f"NODE {i+1}")
            self.nodes.append(n)
            # Lignes de connexion
            self.canvas.create_line(400, 120, pos, 260, dash=(4, 2), fill="gray")

        # 2. Zone de Logs
        self.log_area = scrolledtext.ScrolledText(root, height=8, state='disabled', font=("Consolas", 9))
        self.log_area.pack(fill=tk.X, padx=10, pady=5)

        # 3. Zone de Contrôle (Boutons)
        btn_frame = tk.Frame(root)
        btn_frame.pack(fill=tk.X, pady=10)

        tk.Button(btn_frame, text=" SCÉNARIO: SUCCÈS", bg="#2ecc71", fg="white", font=("Arial", 10, "bold"),
                  command=lambda: self.start_simulation("SUCCESS")).pack(side=tk.LEFT, padx=20, expand=True)

        tk.Button(btn_frame, text=" SCÉNARIO: ÉCHEC VOTE", bg="#f39c12", fg="white", font=("Arial", 10, "bold"),
                  command=lambda: self.start_simulation("FAIL_VOTE")).pack(side=tk.LEFT, padx=20, expand=True)

        tk.Button(btn_frame, text=" SCÉNARIO: CRASH PRE-COMMIT", bg="#e74c3c", fg="white", font=("Arial", 10, "bold"),
                  command=lambda: self.start_simulation("FAIL_PRE")).pack(side=tk.LEFT, padx=20, expand=True)

    def log(self, message):
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')

    def reset_ui(self):
        self.coord_node.set_state("IDLE", COLOR_COORD)
        for n in self.nodes:
            n.set_state("IDLE", COLOR_DEFAULT)
        self.log_area.config(state='normal')
        self.log_area.delete(1.0, tk.END)
        self.log_area.config(state='disabled')

    def start_simulation(self, scenario):
        # On lance dans un thread séparé pour ne pas figer l'interface
        threading.Thread(target=self.run_logic, args=(scenario,)).start()

    def run_logic(self, scenario):
        self.reset_ui()
        self.log(f"--- DÉBUT SCÉNARIO : {scenario} ---")
        time.sleep(1)

        # ================= PHASE 1 : VOTING =================
        self.log(" PHASE 1 : CAN_COMMIT? (Vote demandé)")
        self.coord_node.set_state("WAITING VOTES", COLOR_WAIT)

        for n in self.nodes:
            n.set_state("VOTING...", COLOR_WAIT)
        time.sleep(1.5)

        # Logique de vote selon le scénario
        votes = []
        for i, n in enumerate(self.nodes):
            if scenario == "FAIL_VOTE" and i == 1: # Le Node 2 refuse
                n.set_state("VOTE: NO", COLOR_ABORT)
                self.log(f"   -> Node {i+1} a voté NON (Refus transaction)")
                votes.append(False)
            else:
                n.set_state("VOTE: YES", COLOR_PRE)
                votes.append(True)
            time.sleep(0.5)

        if not all(votes):
            self.log(" DÉCISION : ABORT GLOBAL (Un vote négatif)")
            self.coord_node.set_state("SENDING ABORT", COLOR_ABORT)
            time.sleep(1)
            for n in self.nodes:
                n.set_state("ABORTED", COLOR_ABORT)
            return # FIN DU SCENARIO

        # ================= PHASE 2 : PRE-COMMIT =================
        self.log(" PHASE 2 : PRE-COMMIT (Préparez-vous)")
        self.coord_node.set_state("PRE-COMMIT", COLOR_PRE)
        time.sleep(1.0)

        acks = []
        for i, n in enumerate(self.nodes):
            if scenario == "FAIL_PRE" and i == 2: # Le Node 3 plante
                n.set_state("TIMEOUT / CRASH", "black") # Noir pour mort
                self.log(f"   -> Node {i+1} ne répond pas (TIMEOUT)")
                acks.append(False)
            else:
                n.set_state("ACK PRE-COMMIT", COLOR_PRE)
                self.log(f"   -> Node {i+1} a confirmé (ACK)")
                acks.append(True)
            time.sleep(0.5)

        if not all(acks):
            self.log(" DÉCISION : ABORT GLOBAL (Manque un ACK)")
            self.coord_node.set_state("SENDING ABORT", COLOR_ABORT)
            time.sleep(1)
            for i, n in enumerate(self.nodes):
                # Seuls les vivants reçoivent l'abort
                if not (scenario == "FAIL_PRE" and i == 2):
                    n.set_state("ROLLBACK", COLOR_ABORT)
            return # FIN DU SCENARIO

        # ================= PHASE 3 : DO-COMMIT =================
        self.log(" PHASE 3 : DO-COMMIT (Validation finale)")
        self.coord_node.set_state("COMMITTING", COLOR_COMMIT)
        time.sleep(1)

        for n in self.nodes:
            n.set_state("COMMITTED", COLOR_COMMIT)
            self.log(f"   -> Node {n.id} : Transaction Validée")
            time.sleep(0.3)

        self.log(" SUCCÈS DE LA TRANSACTION DISTRIBUÉE")

if __name__ == "__main__":
    root = tk.Tk()
    app = SimulatorApp(root)
    root.mainloop()