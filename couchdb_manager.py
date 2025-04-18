#!/usr/bin/env python3
"""
Enhanced CouchDB Manager Desktop App with Modern UI
Features:
- Improved UI with modern Ttk theme and consistent layout
- Toggle Connect/Disconnect
- List databases with enhanced visuals
- Add/Delete/Rename Database
- Create/Delete Index on selected database
- Select All / Delete Selected / Delete All databases
- Browse and Edit documents in a selected database
- Status bar showing connection status and operations
Requires: requests, tkinter (built-in)
Usage: python couchdb_manager_enhanced.py
"""
import tkinter as tk
from tkinter import messagebox, ttk, simpledialog, font
import traceback
import requests
from requests.auth import HTTPBasicAuth
import json
import os
from functools import partial

class ModernDialog(tk.simpledialog.Dialog):
    """Enhanced dialog with modern look and feel"""
    def __init__(self, parent, title, message=None):
        self.message = message
        super().__init__(parent, title)
        
    def body(self, master):
        if self.message:
            ttk.Label(master, text=self.message, wraplength=300).grid(row=0, column=0, columnspan=2, pady=(0, 10), sticky="w")
        return master

class InputDialog(ModernDialog):
    """Modern input dialog for getting user input"""
    def __init__(self, parent, title, message=None, initial_value=""):
        self.result = None
        self.initial_value = initial_value
        super().__init__(parent, title, message)
        
    def body(self, master):
        super().body(master)
        self.entry = ttk.Entry(master, width=40)
        self.entry.grid(row=1, column=0, columnspan=2, padx=5, pady=5)
        self.entry.insert(0, self.initial_value)
        self.entry.focus_set()
        return self.entry
    
    def apply(self):
        self.result = self.entry.get()

class FieldsInputDialog(ModernDialog):
    """Dialog for inputting multiple fields for indexes"""
    def __init__(self, parent, title, message=None):
        self.name = ""
        self.fields = ""
        super().__init__(parent, title, message)
    
    def body(self, master):
        super().body(master)
        ttk.Label(master, text="Index Name:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.name_entry = ttk.Entry(master, width=30)
        self.name_entry.grid(row=1, column=1, padx=5, pady=5)
        
        ttk.Label(master, text="Fields (comma-separated):").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.fields_entry = ttk.Entry(master, width=30)
        self.fields_entry.grid(row=2, column=1, padx=5, pady=5)
        
        self.name_entry.focus_set()
        return self.name_entry
    
    def apply(self):
        self.name = self.name_entry.get()
        self.fields = self.fields_entry.get()

class CouchDBManager(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CouchDB Manager")
        self.geometry("1000x700")
        self.minsize(800, 600)
        
        # Set style
        self.style = ttk.Style()
        try:
            self.style.theme_use("clam")  # Try to use a modern theme
        except tk.TclError:
            pass  # Use default theme if clam is not available
        
        self.configure(background="#f0f0f0")
        self.connected = False
        
        # Configure fonts
        self.header_font = font.Font(family="Helvetica", size=12, weight="bold")
        self.normal_font = font.Font(family="Helvetica", size=10)
        
        self._build_ui()

    def _build_ui(self):
        # Main container with padding
        main_frame = ttk.Frame(self, padding="10 10 10 10")
        main_frame.pack(fill="both", expand=True)
        
        # Top frame for application title
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(title_frame, text="CouchDB Manager", font=self.header_font).pack(side="left")
        
        # Connection frame
        conn_frame = ttk.LabelFrame(main_frame, text="Connection Settings", padding="10 10 10 10")
        conn_frame.pack(fill="x", pady=(0, 10))
        
        # Grid layout for connection settings
        ttk.Label(conn_frame, text="CouchDB URL:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        self.url_var = tk.StringVar(value="http://127.0.0.1:5984")
        ttk.Entry(conn_frame, textvariable=self.url_var, width=40).grid(row=0, column=1, columnspan=2, sticky="we", padx=5, pady=5)
        
        ttk.Label(conn_frame, text="Username:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        self.user_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.user_var).grid(row=1, column=1, sticky="we", padx=5, pady=5)
        
        ttk.Label(conn_frame, text="Password:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        self.pass_var = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.pass_var, show="*").grid(row=2, column=1, sticky="we", padx=5, pady=5)
        
        self.conn_btn = ttk.Button(conn_frame, text="Connect", command=self.toggle_connection)
        self.conn_btn.grid(row=3, column=1, sticky="w", padx=5, pady=10)
        
        # Configure column weights for connection frame
        conn_frame.columnconfigure(1, weight=1)
        
        # Content frame with database list and actions
        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill="both", expand=True, pady=(0, 10))
        content_frame.columnconfigure(0, weight=3)
        content_frame.columnconfigure(1, weight=1)
        content_frame.rowconfigure(0, weight=1)
        
        # Database list frame with Treeview instead of Listbox
        list_frame = ttk.LabelFrame(content_frame, text="Databases", padding="10 10 10 10")
        list_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        
        # Create Treeview with scrollbar
        self.tree = ttk.Treeview(list_frame, columns=("name",), show="headings", selectmode="extended")
        self.tree.heading("name", text="Database Name")
        self.tree.column("name", width=200)
        self.tree.pack(side="left", fill="both", expand=True)
        
        # Add vertical scrollbar
        vsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        vsb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=vsb.set)
        
        # Add right panel with action buttons grouped by function
        action_frame = ttk.LabelFrame(content_frame, text="Operations", padding="10 10 10 10")
        action_frame.grid(row=0, column=1, sticky="nsew")
        
        # Group 1: Database Actions
        db_actions = ttk.LabelFrame(action_frame, text="Database Actions", padding="5 5 5 5")
        db_actions.pack(fill="x", pady=(0, 10))
        
        self.action_buttons = {}
        btns = [
            ("Add Database", self.add_db),
            ("Browse Database", self.browse_db),
            ("Rename Database", self.rename_db),
        ]
        
        for i, (txt, cmd) in enumerate(btns):
            b = ttk.Button(db_actions, text=txt, command=cmd, state="disabled")
            b.pack(fill="x", pady=2)
            self.action_buttons[txt] = b
        
        # Group 2: Selection Actions
        sel_actions = ttk.LabelFrame(action_frame, text="Selection Actions", padding="5 5 5 5")
        sel_actions.pack(fill="x", pady=(0, 10))
        
        btns = [
            ("Select All", self.select_all),
            ("Delete Selected", self.delete_selected),
            ("Delete All", self.delete_all),
        ]
        
        for i, (txt, cmd) in enumerate(btns):
            b = ttk.Button(sel_actions, text=txt, command=cmd, state="disabled")
            b.pack(fill="x", pady=2)
            self.action_buttons[txt] = b
        
        # Group 3: Index Actions
        index_actions = ttk.LabelFrame(action_frame, text="Index Actions", padding="5 5 5 5")
        index_actions.pack(fill="x")
        
        btns = [
            ("Create Index", self.create_index),
        ]
        
        for i, (txt, cmd) in enumerate(btns):
            b = ttk.Button(index_actions, text=txt, command=cmd, state="disabled")
            b.pack(fill="x", pady=2)
            self.action_buttons[txt] = b
        
        # Status bar
        self.status_var = tk.StringVar(value="Not connected")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief="sunken", anchor="w", padding="5 2 5 2")
        status_bar.pack(fill="x", side="bottom")

    # Connection logic
    def toggle_connection(self):
        if not self.connected:
            self.connect()
        else:
            self.disconnect()

    def connect(self):
        url = self.url_var.get().rstrip('/')
        user, password = self.user_var.get().strip(), self.pass_var.get().strip()
        
        self.status_var.set("Connecting...")
        self.update()
        
        try:
            resp = requests.get(f"{url}/_all_dbs", auth=HTTPBasicAuth(user, password))
            resp.raise_for_status()
            dbs = resp.json()
            
            # Clear and populate the treeview
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            for db in dbs:
                self.tree.insert("", "end", values=(db,))
            
            # Enable action buttons
            for b in self.action_buttons.values(): 
                b.config(state="normal")
            
            self.conn_btn.config(text="Disconnect")
            self.connected = True
            self.status_var.set(f"Connected to {url} - {len(dbs)} databases found")
        except Exception as e:
            self.status_var.set("Connection failed")
            messagebox.showerror("Connection Error", f"Failed to fetch databases:\n{e}")

    def disconnect(self):
        # Clear the treeview
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Disable action buttons
        for b in self.action_buttons.values(): 
            b.config(state="disabled")
        
        self.conn_btn.config(text="Connect")
        self.connected = False
        self.status_var.set("Not connected")

    # Database operations
    def select_all(self):
        for item in self.tree.get_children():
            self.tree.selection_add(item)

    def delete_selected(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("Info", "No databases selected.")
            return
        
        if not messagebox.askyesno("Confirm", f"Delete {len(selected)} selected database(s)?\nThis action cannot be undone."):
            return
        
        url = self.url_var.get().rstrip('/')
        auth = HTTPBasicAuth(self.user_var.get().strip(), self.pass_var.get().strip())
        
        self.status_var.set(f"Deleting {len(selected)} database(s)...")
        self.update()
        
        success_count = 0
        fail_count = 0
        
        for item in selected:
            db = self.tree.item(item, "values")[0]
            try:
                resp = requests.delete(f"{url}/{db}", auth=auth)
                resp.raise_for_status()
                self.tree.delete(item)
                success_count += 1
            except Exception as e:
                fail_count += 1
                print(f"Failed to delete '{db}': {e}")
        
        self.status_var.set(f"Deleted {success_count} database(s). Failed: {fail_count}")
        if fail_count > 0:
            messagebox.showwarning("Delete Result", f"Successfully deleted {success_count} database(s).\nFailed to delete {fail_count} database(s).")

    def delete_all(self):
        all_items = self.tree.get_children()
        if not all_items:
            messagebox.showinfo("Info", "No databases to delete.")
            return
        
        count = len(all_items)
        if not messagebox.askyesno("Confirm", f"Delete ALL {count} databases? This action cannot be undone."):
            return
        
        url = self.url_var.get().rstrip('/')
        auth = HTTPBasicAuth(self.user_var.get().strip(), self.pass_var.get().strip())
        
        self.status_var.set(f"Deleting all {count} database(s)...")
        self.update()
        
        success_count = 0
        fail_count = 0
        
        for item in all_items:
            db = self.tree.item(item, "values")[0]
            try:
                resp = requests.delete(f"{url}/{db}", auth=auth)
                resp.raise_for_status()
                self.tree.delete(item)
                success_count += 1
            except Exception as e:
                fail_count += 1
                print(f"Failed to delete '{db}': {e}")
        
        self.status_var.set(f"Deleted {success_count} database(s). Failed: {fail_count}")
        if fail_count > 0:
            messagebox.showwarning("Delete Result", f"Successfully deleted {success_count} database(s).\nFailed to delete {fail_count} database(s).")

    def add_db(self):
        dialog = InputDialog(self, "Add Database", "Enter new database name:")
        if not dialog.result:
            return
        
        name = dialog.result
        url = self.url_var.get().rstrip('/')
        auth = HTTPBasicAuth(self.user_var.get().strip(), self.pass_var.get().strip())
        
        self.status_var.set(f"Creating database '{name}'...")
        self.update()
        
        try:
            resp = requests.put(f"{url}/{name}", auth=auth)
            resp.raise_for_status()
            self.tree.insert("", "end", values=(name,))
            self._create_index(name, auth)
            self.status_var.set(f"Created database '{name}' with default index")
        except Exception as e:
            self.status_var.set(f"Failed to create database '{name}'")
            messagebox.showerror("Error", f"Failed to create database:\n{e}")

    def rename_db(self):
        selected = self.tree.selection()
        if not selected or len(selected) != 1:
            messagebox.showinfo("Info", "Select a single database to rename.")
            return
        
        old_name = self.tree.item(selected[0], "values")[0]
        dialog = InputDialog(self, "Rename Database", f"New name for '{old_name}':", old_name)
        
        if not dialog.result or dialog.result == old_name:
            return
        
        new_name = dialog.result
        messagebox.showinfo("Not implemented", "Database rename functionality is under development.\n"
                           "CouchDB does not natively support renaming databases.\n"
                           "You would need to replicate the old DB to a new one with the desired name,\n"
                           "then delete the old one.")
        self.status_var.set("Rename operation not implemented")

    def create_index(self):
        selected = self.tree.selection()
        if not selected or len(selected) != 1:
            messagebox.showinfo("Info", "Select a single database to create an index.")
            return
        
        db = self.tree.item(selected[0], "values")[0]
        auth = HTTPBasicAuth(self.user_var.get().strip(), self.pass_var.get().strip())
        
        dialog = FieldsInputDialog(self, "Create Index", f"Create a new index in database '{db}':")
        if not dialog.name or not dialog.fields:
            return
        
        name = dialog.name
        fields = dialog.fields
        
        self.status_var.set(f"Creating index '{name}' in '{db}'...")
        self.update()
        
        try:
            payload = {"index": {"fields": [f.strip() for f in fields.split(',')]}, "name": name}
            resp = requests.post(f"{self.url_var.get().rstrip('/')}/{db}/_index", 
                               json=payload, auth=auth)
            resp.raise_for_status()
            messagebox.showinfo("Index Created", f"Index '{name}' created in '{db}'.")
            self.status_var.set(f"Created index '{name}' in '{db}'")
        except Exception as e:
            self.status_var.set(f"Failed to create index '{name}'")
            messagebox.showerror("Error", f"Failed to create index:\n{e}")

    def _create_index(self, db, auth):
        payload = {"index": {"fields": ["_id"]}, "name": f"{db}_idx"}
        try:
            requests.post(f"{self.url_var.get().rstrip('/')}/{db}/_index", json=payload, auth=auth)
        except Exception as e:
            print(f"Failed to create default index for '{db}': {e}")

    def browse_db(self):
        selected = self.tree.selection()
        if not selected or len(selected) != 1:
            messagebox.showinfo("Info", "Please select a single database to browse.")
            return
        
        db_name = self.tree.item(selected[0], "values")[0]
        BrowseWindow(self, db_name, self.url_var.get().rstrip('/'),
                     HTTPBasicAuth(self.user_var.get().strip(), self.pass_var.get().strip()))


class BrowseWindow(tk.Toplevel):
    def __init__(self, parent, db_name, base_url, auth):
        super().__init__(parent)
        self.title(f"Browse & Edit: {db_name}")
        self.geometry("1000x700")
        self.minsize(800, 600)
        
        self.db = db_name
        self.base_url = base_url
        self.auth = auth
        self.docs = {}
        self.current_doc = None
        
        # Configure fonts
        self.header_font = font.Font(family="Helvetica", size=11, weight="bold")
        self.normal_font = font.Font(family="Helvetica", size=10)
        self.code_font = font.Font(family="Courier", size=10)
        
        self._build_ui()
        self.status_var = tk.StringVar(value=f"Loading database '{db_name}'...")
        
        # Load data
        self.after(100, self.load_indexes)
        self.after(200, self.load_docs)

    def _build_ui(self):
        # Main frame with padding
        main_frame = ttk.Frame(self, padding="10 10 10 10")
        main_frame.pack(fill="both", expand=True)
        
        # Database info header
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(header_frame, text=f"Database: {self.db}", font=self.header_font).pack(side="left")
        
        # Index management frame
        idx_frame = ttk.LabelFrame(main_frame, text="Indexes", padding="10 10 10 10")
        idx_frame.pack(fill="x", pady=(0, 10))
        
        # Split index frame into list and buttons
        idx_list_frame = ttk.Frame(idx_frame)
        idx_list_frame.pack(side="left", fill="both", expand=True)
        
        # Use Treeview for indexes instead of Listbox
        columns = ("ddoc", "name", "fields")
        self.idx_tree = ttk.Treeview(idx_list_frame, columns=columns, show="headings", height=4)
        
        # Configure columns
        self.idx_tree.heading("ddoc", text="Design Document")
        self.idx_tree.heading("name", text="Index Name")
        self.idx_tree.heading("fields", text="Fields")
        
        self.idx_tree.column("ddoc", width=150)
        self.idx_tree.column("name", width=150)
        self.idx_tree.column("fields", width=250)
        
        self.idx_tree.pack(side="left", fill="both", expand=True)
        
        # Add scrollbar
        idx_scroll = ttk.Scrollbar(idx_list_frame, orient="vertical", command=self.idx_tree.yview)
        idx_scroll.pack(side="right", fill="y")
        self.idx_tree.config(yscrollcommand=idx_scroll.set)
        
        # Index action buttons
        btn_idx = ttk.Frame(idx_frame, padding="10 0 0 0")
        btn_idx.pack(side="right", fill="y")
        
        ttk.Button(btn_idx, text="Add Index", command=self.add_index).pack(fill="x", pady=2)
        ttk.Button(btn_idx, text="Delete Index", command=self.delete_index).pack(fill="x", pady=2)
        ttk.Button(btn_idx, text="Refresh Indexes", command=self.load_indexes).pack(fill="x", pady=2)
        
        # Create content area with paned window
        paned = ttk.PanedWindow(main_frame, orient="horizontal")
        paned.pack(fill="both", expand=True, pady=(0, 10))
        
        # Left side: document list with search
        left_frame = ttk.Frame(paned)
        
        # Search box
        search_frame = ttk.Frame(left_frame)
        search_frame.pack(fill="x", pady=(0, 5))
        
        ttk.Label(search_frame, text="Search:").pack(side="left", padx=(0, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        search_entry.pack(side="left", fill="x", expand=True)
        search_entry.bind("<KeyRelease>", self.filter_docs)
        
        # Document list with Treeview
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill="both", expand=True)
        
        self.doc_tree = ttk.Treeview(list_frame, columns=("id", "rev"), show="headings")
        self.doc_tree.heading("id", text="Document ID")
        self.doc_tree.heading("rev", text="Revision")
        
        self.doc_tree.column("id", width=200)
        self.doc_tree.column("rev", width=100)
        
        self.doc_tree.pack(side="left", fill="both", expand=True)
        
        # Document list scrollbar
        sb = ttk.Scrollbar(list_frame, orient="vertical", command=self.doc_tree.yview)
        sb.pack(side="right", fill="y")
        self.doc_tree.config(yscrollcommand=sb.set)
        self.doc_tree.bind('<<TreeviewSelect>>', self.show_doc)
        
        # Action buttons for documents
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill="x", pady=(5, 0))
        
        ttk.Button(btn_frame, text="New Document", command=self.new_doc).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Delete Document", command=self.delete_doc).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Refresh", command=self.load_docs).pack(side="left", padx=2)
        
        # Add left frame to paned window
        paned.add(left_frame, weight=2)
        
        # Right side: document editor
        right_frame = ttk.Frame(paned)
        
        # Editor frame
        editor_frame = ttk.LabelFrame(right_frame, text="Document Editor", padding="5 5 5 5")
        editor_frame.pack(fill="both", expand=True, pady=(0, 5))
        
        # Use Text widget with syntax highlighting (basic)
        self.text = tk.Text(editor_frame, wrap="none", font=self.code_font)
        self.text.pack(fill="both", expand=True, side="left")
        
        # Add scrollbars
        y_scroll = ttk.Scrollbar(editor_frame, orient="vertical", command=self.text.yview)
        y_scroll.pack(side="right", fill="y")
        
        x_scroll = ttk.Scrollbar(right_frame, orient="horizontal", command=self.text.xview)
        x_scroll.pack(fill="x", side="top")
        
        self.text.config(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        
        # Save button
        save_frame = ttk.Frame(right_frame)
        save_frame.pack(fill="x", pady=(0, 5))
        
        self.save_btn = ttk.Button(save_frame, text="Save Changes", command=self.save_doc)
        self.save_btn.pack(side="right", padx=5)
        
        ttk.Button(save_frame, text="Format JSON", command=self.format_json).pack(side="right", padx=5)
        
        # Add right frame to paned window
        paned.add(right_frame, weight=3)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief="sunken", anchor="w", padding="5 2 5 2")
        status_bar.pack(fill="x", side="bottom")

    def load_indexes(self):
        self.status_var.set("Loading indexes...")
        self.update()
        
        try:
            url = f"{self.base_url}/{self.db}/_index"
            self.status_var.set(f"Fetching from: {url}")
            self.update()
            
            resp = requests.get(url, auth=self.auth)
            resp.raise_for_status()  # Ensure we get a valid response
            
            # Print raw response for debugging
            print(f"Response status: {resp.status_code}")
            print(f"Response text: {resp.text[:200]}...")  # Print first 200 chars
            
            data = resp.json()
            indexes = data.get('indexes', [])
            
            print(f"Found {len(indexes)} indexes")
            
            # Clear existing items
            for item in self.idx_tree.get_children():
                self.idx_tree.delete(item)
            
            # Add indexes to tree with careful handling of None values
            for idx in indexes:
                # Debug print each index
                print(f"Processing index: {idx}")
                
                # Safely extract values with defaults
                ddoc = idx.get('ddoc', '')
                name = idx.get('name', '')
                
                # Handle None values explicitly
                if ddoc is None:
                    ddoc = ''
                else:
                    # Only call replace if ddoc is not None
                    ddoc = str(ddoc).replace('_design/', '')
                
                # Safely get fields with defensive coding
                fields_list = []
                try:
                    fields_def = idx.get('def', {})
                    if fields_def and isinstance(fields_def, dict):
                        fields = fields_def.get('fields', [])
                        if fields and isinstance(fields, list):
                            for f in fields:
                                if isinstance(f, dict):
                                    # Handle case where field is in format {"fieldname": "asc/desc"}
                                    for k in f:
                                        fields_list.append(f"{k}:{f[k]}")
                                else:
                                    fields_list.append(str(f))
                except Exception as e:
                    print(f"Error processing fields: {e}")
                    fields_list = ["<error>"]
                
                fields_str = ", ".join(fields_list)
                
                # Insert into tree with debugging
                print(f"Inserting: ddoc='{ddoc}', name='{name}', fields='{fields_str}'")
                self.idx_tree.insert("", "end", values=(ddoc, name, fields_str))
            
            self.status_var.set(f"Loaded {len(indexes)} indexes")
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error: {str(e)}"
            print(error_msg)
            self.status_var.set(error_msg[:50] + "...")
            messagebox.showerror("Connection Error", error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON response: {str(e)}"
            print(error_msg)
            self.status_var.set(error_msg[:50] + "...")
            messagebox.showerror("JSON Error", error_msg)
        except Exception as e:
            error_msg = f"Error loading indexes: {str(e)}"
            print(f"Exception details: {type(e).__name__}: {e}")
            print(f"Exception traceback: {traceback.format_exc()}")
            self.status_var.set(error_msg[:50] + "...")
            messagebox.showerror("Index Loading Error", error_msg)

    def add_index(self):
        dialog = FieldsInputDialog(self, "Add Index", f"Create new index in database '{self.db}':")
        if not dialog.name or not dialog.fields:
            return
            
        name = dialog.name
        fields = dialog.fields
        
        self.status_var.set(f"Creating index '{name}'...")
        self.update()
        
        try:
            payload = {"index": {"fields": [f.strip() for f in fields.split(',')]}, "name": name}
            resp = requests.post(f"{self.base_url}/{self.db}/_index", json=payload, auth=self.auth)
            resp.raise_for_status()
            self.load_indexes()
            self.status_var.set(f"Created index '{name}'")
        except Exception as e:
            self.status_var.set(f"Failed to create index: {str(e)[:50]}")
            messagebox.showerror("Error", f"Failed to create index:\n{e}")

    def delete_index(self):
        selected = self.idx_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "Select an index to delete.")
            return
            
        idx = self.idx_tree.item(selected[0], "values")
        if len(idx) < 2:
            messagebox.showerror("Error", "Invalid index selected")
            return
            
        ddoc, name = idx[0], idx[1]
        if not messagebox.askyesno("Confirm", f"Delete index '{name}'?"):
            return
            
        self.status_var.set(f"Deleting index '{name}'...")
        self.update()
        
        try:
            ddoc_path = ddoc if ddoc.startswith('_design/') else f"_design/{ddoc}"
            url = f"{self.base_url}/{self.db}/_index/{ddoc_path}/json/{name}"
            resp = requests.delete(url, auth=self.auth)
            resp.raise_for_status()
            self.load_indexes()
            self.status_var.set(f"Deleted index '{name}'")
        except Exception as e:
            self.status_var.set(f"Failed to delete index: {str(e)[:50]}")
            messagebox.showerror("Error", f"Failed to delete index:\n{e}")

    def load_docs(self):
        self.status_var.set("Loading documents...")
        self.update()
        
        try:
            url = f"{self.base_url}/{self.db}/_all_docs?include_docs=true"
            resp = requests.get(url, auth=self.auth)
            data = resp.json()
            
            # Clear existing items
            for item in self.doc_tree.get_children():
                self.doc_tree.delete(item)
            
            # Store all docs for filtering
            self.docs = {}
            
            # Add documents to the tree
            for row in data.get('rows', []):
                doc_id = row.get('id', '')
                if doc_id.startswith('_design/'):
                    continue  # Skip design documents
                    
                rev = row.get('value', {}).get('rev', '')
                doc = row.get('doc', {})
                
                # Store doc for later access
                self.docs[doc_id] = doc
                
                # Add to tree
                self.doc_tree.insert("", "end", values=(doc_id, rev))
            
            self.status_var.set(f"Loaded {len(self.docs)} documents")
        except Exception as e:
            self.status_var.set(f"Error loading documents: {str(e)[:50]}")
            messagebox.showerror("Document Loading Error", f"Failed to load documents:\n{e}")

    def filter_docs(self, event=None):
        search_text = self.search_var.get().lower()
        
        # Clear current display
        for item in self.doc_tree.get_children():
            self.doc_tree.delete(item)
        
        # Filter and add matching docs
        for doc_id, doc in self.docs.items():
            if search_text in doc_id.lower() or search_text in json.dumps(doc).lower():
                rev = doc.get('_rev', '')
                self.doc_tree.insert("", "end", values=(doc_id, rev))
        
        self.status_var.set(f"Found {len(self.doc_tree.get_children())} matching documents")

    def show_doc(self, event=None):
        selected = self.doc_tree.selection()
        if not selected:
            return
        
        # Get the document ID
        doc_id = self.doc_tree.item(selected[0], "values")[0]
        doc = self.docs.get(doc_id, {})
        
        # Update editor
        self.text.delete("1.0", "end")
        json_text = json.dumps(doc, indent=2)
        self.text.insert("1.0", json_text)
        
        # Store current document
        self.current_doc = doc_id
        self.status_var.set(f"Loaded document '{doc_id}'")

    def new_doc(self):
        # Ask for document ID
        dialog = InputDialog(self, "New Document", "Enter document ID (leave blank for auto-generated):")
        doc_id = dialog.result if dialog.result else None
        
        if doc_id is None and not dialog.result == "":
            # User canceled
            return
        
        # Create empty document template
        doc = {"_id": doc_id} if doc_id else {}
        
        # Clear editor and insert template
        self.text.delete("1.0", "end")
        self.text.insert("1.0", json.dumps(doc, indent=2))
        
        # Save new document
        try:
            self.current_doc = None  # Mark as new doc
            self.save_doc()
        except Exception as e:
            self.status_var.set(f"Failed to create document: {str(e)[:50]}")
            messagebox.showerror("Error", f"Failed to create document:\n{e}")

    def delete_doc(self):
        selected = self.doc_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "Select a document to delete.")
            return
        
        doc_id = self.doc_tree.item(selected[0], "values")[0]
        rev = self.doc_tree.item(selected[0], "values")[1]
        
        if not messagebox.askyesno("Confirm", f"Delete document '{doc_id}'?\nThis action cannot be undone."):
            return
        
        self.status_var.set(f"Deleting document '{doc_id}'...")
        self.update()
        
        try:
            url = f"{self.base_url}/{self.db}/{doc_id}?rev={rev}"
            resp = requests.delete(url, auth=self.auth)
            resp.raise_for_status()
            
            # Remove from tree and cache
            self.doc_tree.delete(selected[0])
            if doc_id in self.docs:
                del self.docs[doc_id]
            
            self.status_var.set(f"Deleted document '{doc_id}'")
            
            # Clear editor if showing deleted doc
            if self.current_doc == doc_id:
                self.text.delete("1.0", "end")
                self.current_doc = None
        except Exception as e:
            self.status_var.set(f"Failed to delete document: {str(e)[:50]}")
            messagebox.showerror("Error", f"Failed to delete document:\n{e}")

    def save_doc(self):
        # Get document from editor
        try:
            doc_text = self.text.get("1.0", "end").strip()
            doc = json.loads(doc_text)
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON:\n{e}")
            return
        
        # Check if it's a new or existing document
        if self.current_doc:
            # Ensure _id matches current doc
            if '_id' in doc and doc['_id'] != self.current_doc:
                if not messagebox.askyesno("Warning", 
                                         f"Document ID changed from '{self.current_doc}' to '{doc['_id']}'.\n"
                                         "This will create a new document. Continue?"):
                    return
            else:
                # Set _id if missing
                doc['_id'] = self.current_doc
        
        doc_id = doc.get('_id', '')
        self.status_var.set(f"Saving document '{doc_id}'...")
        self.update()
        
        try:
            url = f"{self.base_url}/{self.db}"
            if not doc_id:
                url = f"{url}"  # Let CouchDB generate ID
            else:
                url = f"{url}/{doc_id}"
            
            resp = requests.put(url, json=doc, auth=self.auth)
            resp.raise_for_status()
            result = resp.json()
            
            if result.get('ok'):
                new_id = result.get('id')
                new_rev = result.get('rev')
                
                # Update the document in our cache
                doc['_id'] = new_id
                doc['_rev'] = new_rev
                self.docs[new_id] = doc
                
                # Refresh the doc tree if needed
                if not self.current_doc or self.current_doc != new_id:
                    # This is a new document or ID changed
                    self.doc_tree.insert("", "end", values=(new_id, new_rev))
                    self.current_doc = new_id
                else:
                    # Update the existing entry
                    for item in self.doc_tree.get_children():
                        if self.doc_tree.item(item, "values")[0] == new_id:
                            self.doc_tree.item(item, values=(new_id, new_rev))
                            break
                
                # Update text with revised document
                self.text.delete("1.0", "end")
                self.text.insert("1.0", json.dumps(doc, indent=2))
                self.status_var.set(f"Saved document '{new_id}'")
            else:
                self.status_var.set("Save failed: no confirmation from server")
                messagebox.showerror("Save Error", "Failed to save document: No confirmation from server")
        except Exception as e:
            self.status_var.set(f"Failed to save document: {str(e)[:50]}")
            messagebox.showerror("Save Error", f"Failed to save document:\n{e}")

    def format_json(self):
        try:
            doc_text = self.text.get("1.0", "end").strip()
            doc = json.loads(doc_text)
            formatted = json.dumps(doc, indent=2)
            
            self.text.delete("1.0", "end")
            self.text.insert("1.0", formatted)
            self.status_var.set("JSON formatted")
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON:\n{e}")


if __name__ == "__main__":
    app = CouchDBManager()
    app.mainloop()