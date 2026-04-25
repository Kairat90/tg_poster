import tkinter as tk
from queue import Empty
from tkinter import filedialog, messagebox

import customtkinter as ctk


class AutoPosterApp(ctk.CTk):
    def __init__(self, repository, telegram_service, settings, log_queue) -> None:
        super().__init__()
        self.repository = repository
        self.telegram_service = telegram_service
        self.settings = settings
        self.log_queue = log_queue

        self.selected_ad_id: int | None = None
        self.selected_target_id: int | None = None
        self.target_ids_for_ads: list[int] = []
        self.target_ids_for_tab: list[int] = []
        self.ad_ids: list[int] = []
        self._context_menu_target = None

        self.title("Telegram Автопостер")
        self.geometry("1280x820")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("green")

        self.status_var = tk.StringVar(value="Запуск...")
        self._setup_text_actions()
        self._build_layout()
        self.refresh_targets()
        self.refresh_ads()
        self.refresh_logs()
        self._poll_status()
        self._poll_logs()

    def _setup_text_actions(self) -> None:
        self._context_menu = tk.Menu(self, tearoff=0)
        self._context_menu.add_command(label="Вырезать", command=lambda: self._text_action("cut"))
        self._context_menu.add_command(label="Копировать", command=lambda: self._text_action("copy"))
        self._context_menu.add_command(label="Вставить", command=lambda: self._text_action("paste"))
        self._context_menu.add_separator()
        self._context_menu.add_command(label="Выделить все", command=lambda: self._text_action("select_all"))

        self.bind_all("<Control-c>", lambda event: self._handle_text_shortcut("copy"))
        self.bind_all("<Control-C>", lambda event: self._handle_text_shortcut("copy"))
        self.bind_all("<Control-x>", lambda event: self._handle_text_shortcut("cut"))
        self.bind_all("<Control-X>", lambda event: self._handle_text_shortcut("cut"))
        self.bind_all("<Control-v>", lambda event: self._handle_text_shortcut("paste"))
        self.bind_all("<Control-V>", lambda event: self._handle_text_shortcut("paste"))
        self.bind_all("<Control-a>", lambda event: self._handle_text_shortcut("select_all"))
        self.bind_all("<Control-A>", lambda event: self._handle_text_shortcut("select_all"))
        self.bind_all("<Control-KeyPress>", self._handle_layout_independent_shortcut, add="+")
        self.bind_all("<Button-3>", self._show_context_menu, add="+")

    def _handle_text_shortcut(self, action: str) -> str | None:
        widget = self.focus_get()
        if not self._is_text_widget(widget):
            return None

        self._apply_text_action(widget, action)
        return "break"

    def _show_context_menu(self, event) -> str | None:
        widget = event.widget
        if not self._is_text_widget(widget):
            return None

        self._context_menu_target = widget
        self._context_menu.tk_popup(event.x_root, event.y_root)
        return "break"

    def _text_action(self, action: str) -> None:
        widget = self._context_menu_target or self.focus_get()
        if not self._is_text_widget(widget):
            return
        self._apply_text_action(widget, action)

    def _apply_text_action(self, widget, action: str) -> None:
        actions = {
            "copy": "<<Copy>>",
            "cut": "<<Cut>>",
            "paste": "<<Paste>>",
            "select_all": "<<SelectAll>>",
        }
        virtual_event = actions.get(action)
        if virtual_event:
            widget.event_generate(virtual_event)

    def _handle_layout_independent_shortcut(self, event) -> str | None:
        widget = self.focus_get()
        if not self._is_text_widget(widget):
            return None

        action_by_keycode = {
            65: "select_all",
            67: "copy",
            86: "paste",
            88: "cut",
        }
        action = action_by_keycode.get(event.keycode)
        if not action:
            return None

        self._apply_text_action(widget, action)
        return "break"

    def _is_text_widget(self, widget) -> bool:
        if widget is None:
            return False

        class_name = widget.winfo_class()
        return class_name in {"Entry", "Text", "CTkEntry", "CTkTextbox"}

    def _build_layout(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        header = ctk.CTkFrame(self, corner_radius=0)
        header.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        header.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            header,
            text="Telegram Автопостер",
            font=ctk.CTkFont(size=24, weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=16, pady=16)

        ctk.CTkLabel(
            header,
            textvariable=self.status_var,
            font=ctk.CTkFont(size=14),
        ).grid(row=0, column=1, sticky="e", padx=16, pady=16)

        self.tabs = ctk.CTkTabview(self)
        self.tabs.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))
        self.tabs.add("Объявления")
        self.tabs.add("Цели")
        self.tabs.add("Логи")

        self._build_ads_tab()
        self._build_targets_tab()
        self._build_logs_tab()

    def _build_ads_tab(self) -> None:
        tab = self.tabs.tab("Объявления")
        tab.grid_columnconfigure(1, weight=1)
        tab.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(tab)
        left.grid(row=0, column=0, sticky="nsw", padx=(0, 12), pady=12)
        left.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(left, text="Объявления", font=ctk.CTkFont(size=18, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=12, pady=(12, 8)
        )

        self.ads_listbox = tk.Listbox(left, width=36, height=24, exportselection=False)
        self.ads_listbox.grid(row=1, column=0, sticky="nsew", padx=12, pady=8)
        self.ads_listbox.bind("<<ListboxSelect>>", self.on_ad_selected)

        ctk.CTkButton(left, text="Новое объявление", command=self.new_ad).grid(
            row=2, column=0, sticky="ew", padx=12, pady=(8, 6)
        )
        ctk.CTkButton(left, text="Удалить объявление", command=self.delete_ad, fg_color="#8b1e1e").grid(
            row=3, column=0, sticky="ew", padx=12, pady=(0, 12)
        )

        right = ctk.CTkScrollableFrame(tab)
        right.grid(row=0, column=1, sticky="nsew", pady=12)
        right.grid_columnconfigure(1, weight=1)

        row = 0
        ctk.CTkLabel(right, text="Заголовок").grid(row=row, column=0, sticky="w", padx=12, pady=8)
        self.ad_title_entry = ctk.CTkEntry(right)
        self.ad_title_entry.grid(row=row, column=1, sticky="ew", padx=12, pady=8)

        row += 1
        ctk.CTkLabel(right, text="Текст").grid(row=row, column=0, sticky="nw", padx=12, pady=8)
        self.ad_text_box = ctk.CTkTextbox(right, height=180)
        self.ad_text_box.grid(row=row, column=1, sticky="ew", padx=12, pady=8)

        row += 1
        ctk.CTkLabel(right, text="Время публикации").grid(row=row, column=0, sticky="w", padx=12, pady=8)
        self.ad_times_entry = ctk.CTkEntry(right, placeholder_text="09:00, 14:00, 20:00")
        self.ad_times_entry.grid(row=row, column=1, sticky="ew", padx=12, pady=8)

        row += 1
        ctk.CTkLabel(right, text="Интервал в днях").grid(
            row=row, column=0, sticky="w", padx=12, pady=8
        )
        self.ad_interval_entry = ctk.CTkEntry(right)
        self.ad_interval_entry.insert(0, "1")
        self.ad_interval_entry.grid(row=row, column=1, sticky="ew", padx=12, pady=8)

        row += 1
        self.ad_active_var = tk.BooleanVar(value=True)
        ctk.CTkCheckBox(right, text="Объявление активно", variable=self.ad_active_var).grid(
            row=row, column=1, sticky="w", padx=12, pady=8
        )

        row += 1
        ctk.CTkLabel(right, text="Медиафайлы").grid(row=row, column=0, sticky="nw", padx=12, pady=8)
        media_frame = ctk.CTkFrame(right)
        media_frame.grid(row=row, column=1, sticky="ew", padx=12, pady=8)
        media_frame.grid_columnconfigure(0, weight=1)

        self.ad_media_box = ctk.CTkTextbox(media_frame, height=120)
        self.ad_media_box.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 8))

        ctk.CTkButton(media_frame, text="Добавить медиа", command=self.add_media_files).grid(
            row=1, column=0, sticky="w", padx=12, pady=(0, 12)
        )

        row += 1
        ctk.CTkLabel(right, text="Чаты для публикации").grid(
            row=row, column=0, sticky="nw", padx=12, pady=8
        )
        self.ad_target_listbox = tk.Listbox(
            right,
            selectmode=tk.MULTIPLE,
            height=10,
            exportselection=False,
        )
        self.ad_target_listbox.grid(row=row, column=1, sticky="ew", padx=12, pady=8)

        row += 1
        buttons = ctk.CTkFrame(right)
        buttons.grid(row=row, column=1, sticky="ew", padx=12, pady=16)
        for column in range(3):
            buttons.grid_columnconfigure(column, weight=1)

        ctk.CTkButton(buttons, text="Сохранить", command=self.save_ad).grid(
            row=0, column=0, sticky="ew", padx=6, pady=12
        )
        ctk.CTkButton(buttons, text="Опубликовать сейчас", command=self.publish_now).grid(
            row=0, column=1, sticky="ew", padx=6, pady=12
        )
        ctk.CTkButton(buttons, text="Перезагрузить расписание", command=self.reload_scheduler).grid(
            row=0, column=2, sticky="ew", padx=6, pady=12
        )

    def _build_targets_tab(self) -> None:
        tab = self.tabs.tab("Цели")
        tab.grid_columnconfigure(1, weight=1)
        tab.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(tab)
        left.grid(row=0, column=0, sticky="nsw", padx=(0, 12), pady=12)
        left.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(left, text="Цели", font=ctk.CTkFont(size=18, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=12, pady=(12, 8)
        )

        self.targets_listbox = tk.Listbox(left, width=36, height=24, exportselection=False)
        self.targets_listbox.grid(row=1, column=0, sticky="nsew", padx=12, pady=8)
        self.targets_listbox.bind("<<ListboxSelect>>", self.on_target_selected)

        ctk.CTkButton(left, text="Новая цель", command=self.new_target).grid(
            row=2, column=0, sticky="ew", padx=12, pady=(8, 6)
        )
        ctk.CTkButton(
            left,
            text="Удалить цель",
            command=self.delete_target,
            fg_color="#8b1e1e",
        ).grid(row=3, column=0, sticky="ew", padx=12, pady=(0, 12))

        right = ctk.CTkFrame(tab)
        right.grid(row=0, column=1, sticky="nsew", pady=12)
        right.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(right, text="Название").grid(row=0, column=0, sticky="w", padx=12, pady=12)
        self.target_name_entry = ctk.CTkEntry(right)
        self.target_name_entry.grid(row=0, column=1, sticky="ew", padx=12, pady=12)

        ctk.CTkLabel(right, text="Чат / ссылка").grid(row=1, column=0, sticky="w", padx=12, pady=12)
        self.target_chat_ref_entry = ctk.CTkEntry(right, placeholder_text="@channel или https://t.me/...")
        self.target_chat_ref_entry.grid(row=1, column=1, sticky="ew", padx=12, pady=12)

        self.target_active_var = tk.BooleanVar(value=True)
        ctk.CTkCheckBox(right, text="Цель активна", variable=self.target_active_var).grid(
            row=2, column=1, sticky="w", padx=12, pady=12
        )

        ctk.CTkButton(right, text="Сохранить цель", command=self.save_target).grid(
            row=3, column=1, sticky="w", padx=12, pady=12
        )

    def _build_logs_tab(self) -> None:
        tab = self.tabs.tab("Логи")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(0, weight=1)

        self.logs_box = ctk.CTkTextbox(tab)
        self.logs_box.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        self.logs_box.configure(state="disabled")

        ctk.CTkButton(tab, text="Обновить логи", command=self.refresh_logs).grid(
            row=1, column=0, sticky="e", padx=12, pady=(0, 12)
        )

    def refresh_ads(self) -> None:
        ads = self.repository.list_ads()
        self.ad_ids = [row["id"] for row in ads]

        self.ads_listbox.delete(0, tk.END)
        for row in ads:
            active_mark = "ВКЛ" if row["is_active"] else "ВЫКЛ"
            label = (
                f"[{active_mark}] {row['title']} | каждые {row['interval_days']} дн. | "
                f"{row['schedule_count']} врем. | {row['target_count']} целей"
            )
            self.ads_listbox.insert(tk.END, label)

    def refresh_targets(self) -> None:
        targets = self.repository.list_targets()
        self.target_ids_for_tab = [row["id"] for row in targets]
        self.target_ids_for_ads = [row["id"] for row in targets]

        self.targets_listbox.delete(0, tk.END)
        self.ad_target_listbox.delete(0, tk.END)

        for row in targets:
            active_mark = "ВКЛ" if row["is_active"] else "ВЫКЛ"
            target_label = f"[{active_mark}] {row['name']} -> {row['chat_ref']}"
            self.targets_listbox.insert(tk.END, target_label)
            self.ad_target_listbox.insert(tk.END, target_label)

    def refresh_logs(self) -> None:
        rows = list(reversed(self.repository.list_publish_logs()))
        lines = []
        for row in rows:
            target = row["target_name"] or row["target_chat_ref"] or "неизвестная цель"
            lines.append(
                f"{row['published_at']} | {row['status']} | {row['ad_title']} | {target} | {row['message']}"
            )

        self.logs_box.configure(state="normal")
        self.logs_box.delete("1.0", tk.END)
        self.logs_box.insert("1.0", "\n".join(lines))
        self.logs_box.configure(state="disabled")

    def on_ad_selected(self, _event=None) -> None:
        selection = self.ads_listbox.curselection()
        if not selection:
            return

        self.selected_ad_id = self.ad_ids[selection[0]]
        ad = self.repository.get_ad(self.selected_ad_id)
        if not ad:
            return

        self.ad_title_entry.delete(0, tk.END)
        self.ad_title_entry.insert(0, ad["title"])

        self.ad_text_box.delete("1.0", tk.END)
        self.ad_text_box.insert("1.0", ad["text"])

        self.ad_times_entry.delete(0, tk.END)
        self.ad_times_entry.insert(0, ", ".join(ad["times"]))

        self.ad_interval_entry.delete(0, tk.END)
        self.ad_interval_entry.insert(0, str(ad["interval_days"]))

        self.ad_active_var.set(ad["is_active"])

        self.ad_media_box.delete("1.0", tk.END)
        self.ad_media_box.insert("1.0", "\n".join(ad["media_paths"]))

        self.ad_target_listbox.selection_clear(0, tk.END)
        for index, target_id in enumerate(self.target_ids_for_ads):
            if target_id in ad["target_ids"]:
                self.ad_target_listbox.selection_set(index)

    def on_target_selected(self, _event=None) -> None:
        selection = self.targets_listbox.curselection()
        if not selection:
            return

        self.selected_target_id = self.target_ids_for_tab[selection[0]]
        target = self.repository.get_target(self.selected_target_id)
        if not target:
            return

        self.target_name_entry.delete(0, tk.END)
        self.target_name_entry.insert(0, target["name"])

        self.target_chat_ref_entry.delete(0, tk.END)
        self.target_chat_ref_entry.insert(0, target["chat_ref"])

        self.target_active_var.set(bool(target["is_active"]))

    def add_media_files(self) -> None:
        filenames = filedialog.askopenfilenames(
            title="Выберите изображения",
            filetypes=[("Изображения", "*.jpg *.jpeg *.png *.webp *.gif"), ("Все файлы", "*.*")],
        )
        if not filenames:
            return

        existing = self._get_text_lines(self.ad_media_box)
        merged = existing + [name for name in filenames if name not in existing]
        self.ad_media_box.delete("1.0", tk.END)
        self.ad_media_box.insert("1.0", "\n".join(merged))

    def new_ad(self) -> None:
        self.selected_ad_id = None
        self.ad_title_entry.delete(0, tk.END)
        self.ad_text_box.delete("1.0", tk.END)
        self.ad_times_entry.delete(0, tk.END)
        self.ad_interval_entry.delete(0, tk.END)
        self.ad_interval_entry.insert(0, "1")
        self.ad_media_box.delete("1.0", tk.END)
        self.ad_target_listbox.selection_clear(0, tk.END)
        self.ad_active_var.set(True)
        self.ads_listbox.selection_clear(0, tk.END)

    def save_ad(self) -> None:
        title = self.ad_title_entry.get().strip()
        text = self.ad_text_box.get("1.0", tk.END).strip()
        times = [item.strip() for item in self.ad_times_entry.get().split(",") if item.strip()]
        interval_text = self.ad_interval_entry.get().strip()
        media_sources = self._get_text_lines(self.ad_media_box)
        target_ids = [
            self.target_ids_for_ads[index] for index in self.ad_target_listbox.curselection()
        ]

        if not title:
            messagebox.showerror("Проверка", "Введите заголовок.")
            return
        if not text:
            messagebox.showerror("Проверка", "Введите текст.")
            return
        if not times:
            messagebox.showerror("Проверка", "Добавьте хотя бы одно время публикации.")
            return
        if not target_ids:
            messagebox.showerror("Проверка", "Выберите хотя бы один чат.")
            return

        try:
            interval_days = int(interval_text)
            if interval_days < 1:
                raise ValueError
        except ValueError:
            messagebox.showerror("Проверка", "Интервал в днях должен быть положительным целым числом.")
            return

        try:
            times = self._normalize_times(times)
        except ValueError as exc:
            messagebox.showerror("Проверка", str(exc))
            return

        try:
            ad_id = self.repository.save_ad(
                self.selected_ad_id,
                title,
                text,
                interval_days,
                self.ad_active_var.get(),
                media_sources,
                target_ids,
                times,
            )
        except Exception as exc:
            messagebox.showerror("Ошибка сохранения", str(exc))
            return

        self.selected_ad_id = ad_id
        self.refresh_ads()
        self.refresh_logs()
        self.telegram_service.refresh_scheduler()
        messagebox.showinfo("Сохранено", "Объявление сохранено, расписание обновлено.")

    def delete_ad(self) -> None:
        if self.selected_ad_id is None:
            messagebox.showerror("Удаление", "Сначала выберите объявление.")
            return
        if not messagebox.askyesno("Удаление", "Удалить выбранное объявление?"):
            return

        self.repository.delete_ad(self.selected_ad_id)
        self.telegram_service.refresh_scheduler()
        self.new_ad()
        self.refresh_ads()
        self.refresh_logs()

    def publish_now(self) -> None:
        if self.selected_ad_id is None:
            messagebox.showerror("Публикация", "Сначала выберите объявление.")
            return

        try:
            self.telegram_service.publish_now(self.selected_ad_id)
            messagebox.showinfo("Публикация", "Ручная публикация поставлена в очередь.")
        except Exception as exc:
            messagebox.showerror("Ошибка публикации", str(exc))

    def reload_scheduler(self) -> None:
        self.telegram_service.refresh_scheduler()
        messagebox.showinfo("Расписание", "Запрошено обновление расписания.")

    def new_target(self) -> None:
        self.selected_target_id = None
        self.target_name_entry.delete(0, tk.END)
        self.target_chat_ref_entry.delete(0, tk.END)
        self.target_active_var.set(True)
        self.targets_listbox.selection_clear(0, tk.END)

    def save_target(self) -> None:
        name = self.target_name_entry.get().strip()
        chat_ref = self.target_chat_ref_entry.get().strip()
        if not name or not chat_ref:
            messagebox.showerror("Проверка", "Введите название и чат/ссылку.")
            return

        try:
            self.selected_target_id = self.repository.save_target(
                self.selected_target_id,
                name,
                chat_ref,
                self.target_active_var.get(),
            )
        except Exception as exc:
            messagebox.showerror("Ошибка сохранения", str(exc))
            return

        self.refresh_targets()
        self.refresh_ads()
        self.telegram_service.refresh_scheduler()
        messagebox.showinfo("Сохранено", "Цель сохранена.")

    def delete_target(self) -> None:
        if self.selected_target_id is None:
            messagebox.showerror("Удаление", "Сначала выберите цель.")
            return
        if not messagebox.askyesno("Удаление", "Удалить выбранную цель?"):
            return

        self.repository.delete_target(self.selected_target_id)
        self.new_target()
        self.refresh_targets()
        self.refresh_ads()
        self.telegram_service.refresh_scheduler()

    def _poll_status(self) -> None:
        self.status_var.set(self.telegram_service.get_status())
        self.after(1000, self._poll_status)

    def _poll_logs(self) -> None:
        updated = False
        while True:
            try:
                self.log_queue.get_nowait()
                updated = True
            except Empty:
                break

        if updated:
            self.refresh_logs()

        self.after(2000, self._poll_logs)

    def _get_text_lines(self, textbox: ctk.CTkTextbox) -> list[str]:
        return [line.strip() for line in textbox.get("1.0", tk.END).splitlines() if line.strip()]

    def _normalize_times(self, raw_times: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw_time in raw_times:
            parts = raw_time.split(":", maxsplit=1)
            if len(parts) != 2:
                raise ValueError(f"Неверный формат времени: {raw_time}")

            try:
                hour = int(parts[0])
                minute = int(parts[1])
            except ValueError as exc:
                raise ValueError(f"Неверный формат времени: {raw_time}") from exc

            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError(f"Недопустимое значение времени: {raw_time}")

            normalized.append(f"{hour:02d}:{minute:02d}")

        return sorted(set(normalized))
