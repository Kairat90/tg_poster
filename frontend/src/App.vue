<template>
  <div class="page-shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Telegram Automation</p>
        <h1>TG Poster</h1>
      </div>
      <div class="status-card">
        <span class="status-label">Статус</span>
        <strong>{{ state.status }}</strong>
      </div>
    </header>

    <nav class="tabs">
      <button
        v-for="item in tabs"
        :key="item.id"
        :class="{ active: state.activeTab === item.id }"
        type="button"
        @click="state.activeTab = item.id"
      >
        {{ item.label }}
      </button>
    </nav>

    <div v-if="state.message" class="flash success">
      {{ state.message }}
    </div>
    <div v-if="state.error" class="flash error">
      {{ state.error }}
    </div>

    <section v-if="state.activeTab === 'ads'" class="layout two-column">
      <div class="panel list-panel">
        <div class="panel-header">
          <h2>Объявления</h2>
          <button class="ghost-button" type="button" @click="resetAdForm">Новое объявление</button>
        </div>
        <div class="list-stack">
          <button
            v-for="ad in state.ads"
            :key="ad.id"
            class="list-card as-button"
            :class="{ selected: state.adForm.id === ad.id }"
            type="button"
            @click="selectAd(ad.id)"
          >
            <strong>{{ ad.title }}</strong>
            <span>{{ ad.is_active ? "ВКЛ" : "ВЫКЛ" }} • каждые {{ ad.interval_days }} дн.</span>
            <span>{{ ad.schedule_count }} врем. • {{ ad.target_count }} каналов</span>
          </button>
          <div v-if="!state.ads.length" class="empty-state">Объявлений пока нет.</div>
        </div>
      </div>

      <div class="panel form-panel">
        <div class="panel-header panel-header--stacked">
          <div>
            <h2>{{ state.adForm.id ? "Редактирование объявления" : "Новое объявление" }}</h2>
            <p class="panel-subtitle">Настрой текст, медиа, расписание и каналы публикации.</p>
          </div>
          <div v-if="state.adForm.id" class="toolbar-actions">
            <button type="button" class="secondary" @click="publishAd">Опубликовать сейчас</button>
            <button type="button" class="secondary" @click="reloadScheduler">Обновить расписание</button>
            <button type="button" class="danger" @click="deleteAd">Удалить</button>
          </div>
        </div>

        <form class="form-grid" @submit.prevent="saveAd">
          <label class="field-card">
            <span>Заголовок</span>
            <input v-model="state.adForm.title" type="text" required />
          </label>

          <label class="field-card">
            <span>Интервал в днях</span>
            <input v-model="state.adForm.interval_days" type="number" min="1" required />
            <small>Например, `1` - каждый день, `2` - через день.</small>
          </label>

          <label class="field-card full-width">
            <span>Текст</span>
            <textarea v-model="state.adForm.text" rows="8" required />
          </label>

          <label class="field-card">
            <span>Время публикации</span>
            <input v-model="state.adForm.timesInput" type="text" placeholder="09:00, 14:00, 20:00" required />
            <small>Можно указать несколько значений через запятую.</small>
          </label>

          <label class="checkbox checkbox-card">
            <input v-model="state.adForm.is_active" type="checkbox" />
            <span>Объявление активно</span>
          </label>

          <label class="field-card full-width">
            <span>Текущие медиафайлы</span>
            <textarea
              v-model="state.adForm.existing_media"
              rows="5"
              placeholder="Оставьте пустым, если медиа не нужны"
            />
            <small>Каждый путь с новой строки. При сохранении список будет пересобран.</small>
          </label>

          <label class="field-card full-width">
            <span>Загрузить новые медиафайлы</span>
            <input type="file" multiple @change="onFilesSelected" />
            <small v-if="state.adForm.newFiles.length">Выбрано файлов: {{ state.adForm.newFiles.length }}</small>
            <small v-else>Можно выбрать сразу несколько изображений.</small>
          </label>

          <fieldset class="full-width selection-box">
            <legend>Каналы для публикации</legend>
            <div class="checkbox-grid">
              <label v-for="target in state.targets" :key="target.id" class="checkbox checkbox-grid-item">
                <input
                  :checked="state.adForm.target_ids.includes(target.id)"
                  type="checkbox"
                  @change="toggleTarget(target.id, $event.target.checked)"
                />
                <span>{{ formatTargetLabel(target) }}</span>
              </label>
              <div v-if="!state.targets.length" class="empty-state">
                Сначала добавьте хотя бы один канал на вкладке "Каналы".
              </div>
            </div>
          </fieldset>

          <div class="actions full-width actions--primary">
            <button type="submit" :disabled="state.savingAd">
              {{ state.savingAd ? "Сохранение..." : "Сохранить объявление" }}
            </button>
            <button v-if="state.adForm.id" type="button" class="secondary" @click="resetAdForm">Сбросить выбор</button>
          </div>
        </form>
      </div>
    </section>

    <section v-if="state.activeTab === 'targets'" class="layout two-column">
      <div class="panel list-panel">
        <div class="panel-header">
          <h2>Каналы</h2>
          <button class="ghost-button" type="button" @click="resetTargetForm">Новый канал</button>
        </div>
        <div class="list-stack">
          <button
            v-for="target in state.targets"
            :key="target.id"
            class="list-card as-button"
            :class="{ selected: state.targetForm.id === target.id }"
            type="button"
            @click="selectTarget(target.id)"
          >
            <strong>{{ target.name }}</strong>
            <span>{{ target.is_active ? "ВКЛ" : "ВЫКЛ" }}</span>
            <span>{{ target.chat_ref }}</span>
            <span v-if="target.topic_title" class="topic-badge">Тема: {{ target.topic_title }}</span>
          </button>
          <div v-if="!state.targets.length" class="empty-state">Каналов пока нет.</div>
        </div>
      </div>

      <div class="panel form-panel">
        <div class="panel-header panel-header--stacked">
          <div>
            <h2>{{ state.targetForm.id ? "Редактирование канала" : "Новый канал" }}</h2>
            <p class="panel-subtitle">Храни здесь каналы, чаты и ссылки для публикации. Для групп с темами можно выбрать нужную тему.</p>
          </div>
          <div v-if="state.targetForm.id" class="toolbar-actions">
            <button type="button" class="danger" @click="deleteTarget">Удалить канал</button>
          </div>
        </div>

        <form class="form-grid" @submit.prevent="saveTarget">
          <label class="field-card">
            <span>Название</span>
            <input v-model="state.targetForm.name" type="text" required />
          </label>

          <label class="field-card">
            <span>Чат / ссылка</span>
            <input
              v-model="state.targetForm.chat_ref"
              type="text"
              placeholder="@channel или https://t.me/..."
              required
            />
          </label>

          <div class="field-card full-width">
            <div class="field-row">
              <div>
                <span class="field-label">Тема</span>
                <small>Для обычного канала оставь пусто. Для форума можно загрузить список тем из Telegram.</small>
              </div>
              <button
                type="button"
                class="secondary"
                :disabled="state.loadingTopics || !state.targetForm.chat_ref.trim()"
                @click="fetchTopicsForCurrentTarget"
              >
                {{ state.loadingTopics ? "Загрузка тем..." : "Загрузить темы" }}
              </button>
            </div>

            <div class="topic-controls">
              <select v-model="state.targetForm.topic_id" @change="syncSelectedTopic">
                <option value="">Без темы</option>
                <option v-for="topic in state.targetForm.availableTopics" :key="topic.id" :value="String(topic.id)">
                  {{ topic.title }} ({{ topic.id }})
                </option>
              </select>
              <input v-model="state.targetForm.topic_title" type="text" placeholder="Название темы" readonly />
            </div>

            <small v-if="state.targetForm.topic_id">
              Выбрана тема `{{ state.targetForm.topic_title || "без названия" }}` с ID {{ state.targetForm.topic_id }}.
            </small>
          </div>

          <label class="checkbox checkbox-card full-width">
            <input v-model="state.targetForm.is_active" type="checkbox" />
            <span>Канал активен</span>
          </label>

          <div class="actions full-width actions--primary">
            <button type="submit" :disabled="state.savingTarget">
              {{ state.savingTarget ? "Сохранение..." : "Сохранить канал" }}
            </button>
            <button v-if="state.targetForm.id" type="button" class="secondary" @click="resetTargetForm">Сбросить выбор</button>
          </div>
        </form>
      </div>
    </section>

    <section v-if="state.activeTab === 'logs'" class="panel">
      <div class="panel-header">
        <h2>Логи публикаций</h2>
        <button class="ghost-button" type="button" @click="refreshLogs">Обновить</button>
      </div>
      <div class="log-table-wrapper">
        <table class="log-table">
          <thead>
            <tr>
              <th>Время</th>
              <th>Статус</th>
              <th>Объявление</th>
              <th>Канал</th>
              <th>Сообщение</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in state.logs" :key="row.id">
              <td>{{ row.published_at }}</td>
              <td>{{ row.status }}</td>
              <td>{{ row.ad_title }}</td>
              <td>{{ row.target_name || row.target_chat_ref || "неизвестный канал" }}</td>
              <td>{{ row.message }}</td>
            </tr>
            <tr v-if="!state.logs.length">
              <td colspan="5" class="empty-state">Логов пока нет.</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>

<script setup>
import { onMounted, reactive } from "vue";

const tabs = [
  { id: "ads", label: "Объявления" },
  { id: "targets", label: "Каналы" },
  { id: "logs", label: "Логи" },
];

const emptyAdForm = () => ({
  id: null,
  title: "",
  text: "",
  timesInput: "",
  interval_days: 1,
  is_active: true,
  existing_media: "",
  target_ids: [],
  newFiles: [],
});

const emptyTargetForm = () => ({
  id: null,
  name: "",
  chat_ref: "",
  topic_id: "",
  topic_title: "",
  availableTopics: [],
  is_active: true,
});

const state = reactive({
  status: "Загрузка...",
  message: "",
  error: "",
  activeTab: "ads",
  ads: [],
  targets: [],
  logs: [],
  adForm: emptyAdForm(),
  targetForm: emptyTargetForm(),
  savingAd: false,
  savingTarget: false,
  loadingTopics: false,
});

function setMessage(message = "") {
  state.message = message;
  state.error = "";
}

function setError(message = "") {
  state.error = message;
  state.message = "";
}

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const contentType = response.headers.get("content-type") || "";
  const data = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(data.detail || "Ошибка запроса.");
  }
  return data;
}

function formatTargetLabel(target) {
  const base = `[${target.is_active ? "ВКЛ" : "ВЫКЛ"}] ${target.name} -> ${target.chat_ref}`;
  return target.topic_title ? `${base} | тема: ${target.topic_title}` : base;
}

function assignAdForm(ad) {
  state.adForm = {
    id: ad?.id ?? null,
    title: ad?.title ?? "",
    text: ad?.text ?? "",
    timesInput: ad?.times?.join(", ") ?? "",
    interval_days: ad?.interval_days ?? 1,
    is_active: ad?.is_active ?? true,
    existing_media: ad?.media_paths?.join("\n") ?? "",
    target_ids: ad?.target_ids ? [...ad.target_ids] : [],
    newFiles: [],
  };
}

function assignTargetForm(target) {
  state.targetForm = {
    id: target?.id ?? null,
    name: target?.name ?? "",
    chat_ref: target?.chat_ref ?? "",
    topic_id: target?.topic_id ? String(target.topic_id) : "",
    topic_title: target?.topic_title ?? "",
    availableTopics: [],
    is_active: target?.is_active ?? true,
  };
}

function syncSelectedTopic() {
  if (!state.targetForm.topic_id) {
    state.targetForm.topic_title = "";
    return;
  }
  const selected = state.targetForm.availableTopics.find((topic) => String(topic.id) === state.targetForm.topic_id);
  state.targetForm.topic_title = selected?.title ?? state.targetForm.topic_title;
}

async function bootstrap() {
  try {
    const data = await api("/api/bootstrap");
    state.status = data.status;
    state.ads = data.ads;
    state.targets = data.targets;
    state.logs = data.logs;
    assignAdForm(null);
    assignTargetForm(null);
  } catch (error) {
    setError(error.message);
  }
}

async function selectAd(id) {
  try {
    const data = await api(`/api/ads/${id}`);
    assignAdForm(data.item);
    setMessage("");
    state.activeTab = "ads";
  } catch (error) {
    setError(error.message);
  }
}

async function selectTarget(id) {
  try {
    const data = await api(`/api/targets/${id}`);
    assignTargetForm(data.item);
    if (state.targetForm.chat_ref.trim()) {
      await fetchTopicsForCurrentTarget(false);
    }
    setMessage("");
    state.activeTab = "targets";
  } catch (error) {
    setError(error.message);
  }
}

function resetAdForm() {
  assignAdForm(null);
  setMessage("");
}

function resetTargetForm() {
  assignTargetForm(null);
  setMessage("");
}

function toggleTarget(targetId, checked) {
  if (checked) {
    if (!state.adForm.target_ids.includes(targetId)) {
      state.adForm.target_ids.push(targetId);
    }
    return;
  }
  state.adForm.target_ids = state.adForm.target_ids.filter((id) => id !== targetId);
}

function onFilesSelected(event) {
  state.adForm.newFiles = Array.from(event.target.files || []);
}

async function fetchTopicsForCurrentTarget(showMessage = true) {
  if (!state.targetForm.chat_ref.trim()) {
    setError("Сначала укажи чат или ссылку.");
    return;
  }

  state.loadingTopics = true;
  try {
    const data = await api(`/api/target-topics?chat_ref=${encodeURIComponent(state.targetForm.chat_ref.trim())}`);
    state.targetForm.availableTopics = data.items;
    if (
      state.targetForm.topic_id &&
      !state.targetForm.availableTopics.some((topic) => String(topic.id) === state.targetForm.topic_id)
    ) {
      state.targetForm.topic_id = "";
      state.targetForm.topic_title = "";
    }
    syncSelectedTopic()
    if (showMessage) {
      setMessage(`Загружено тем: ${data.items.length}.`);
    }
  } catch (error) {
    setError(error.message);
  } finally {
    state.loadingTopics = false;
  }
}

async function refreshSummary() {
  const [ads, targets, logs, status] = await Promise.all([
    api("/api/ads"),
    api("/api/targets"),
    api("/api/logs"),
    api("/api/status"),
  ]);
  state.ads = ads.items;
  state.targets = targets.items;
  state.logs = logs.items;
  state.status = status.status;
}

async function saveAd() {
  state.savingAd = true;
  try {
    const formData = new FormData();
    if (state.adForm.id) {
      formData.append("ad_id", state.adForm.id);
    }
    formData.append("title", state.adForm.title);
    formData.append("text", state.adForm.text);
    formData.append("times", state.adForm.timesInput);
    formData.append("interval_days", state.adForm.interval_days);
    formData.append("is_active", String(state.adForm.is_active));
    formData.append("existing_media", state.adForm.existing_media);
    state.adForm.target_ids.forEach((targetId) => formData.append("target_ids", targetId));
    state.adForm.newFiles.forEach((file) => formData.append("media_files", file));

    const data = await api("/api/ads", {
      method: "POST",
      body: formData,
    });

    assignAdForm(data.item);
    await refreshSummary();
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  } finally {
    state.savingAd = false;
  }
}

async function deleteAd() {
  if (!state.adForm.id || !window.confirm("Удалить выбранное объявление?")) {
    return;
  }

  try {
    const data = await api(`/api/ads/${state.adForm.id}`, { method: "DELETE" });
    await refreshSummary();
    resetAdForm();
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  }
}

async function publishAd() {
  if (!state.adForm.id) {
    return;
  }

  try {
    const data = await api(`/api/ads/${state.adForm.id}/publish`, { method: "POST" });
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  }
}

async function saveTarget() {
  state.savingTarget = true;
  try {
    const formData = new FormData();
    if (state.targetForm.id) {
      formData.append("target_id", state.targetForm.id);
    }
    formData.append("name", state.targetForm.name);
    formData.append("chat_ref", state.targetForm.chat_ref);
    formData.append("topic_id", state.targetForm.topic_id);
    formData.append("topic_title", state.targetForm.topic_title);
    formData.append("is_active", String(state.targetForm.is_active));

    const data = await api("/api/targets", {
      method: "POST",
      body: formData,
    });

    assignTargetForm(data.item);
    await refreshSummary();
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  } finally {
    state.savingTarget = false;
  }
}

async function deleteTarget() {
  if (!state.targetForm.id || !window.confirm("Удалить выбранный канал?")) {
    return;
  }

  try {
    const data = await api(`/api/targets/${state.targetForm.id}`, { method: "DELETE" });
    await refreshSummary();
    resetTargetForm();
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  }
}

async function reloadScheduler() {
  try {
    const data = await api("/api/scheduler/reload", { method: "POST" });
    await refreshSummary();
    setMessage(data.message);
  } catch (error) {
    setError(error.message);
  }
}

async function refreshLogs() {
  try {
    const data = await api("/api/logs");
    state.logs = data.items;
    const status = await api("/api/status");
    state.status = status.status;
    setMessage("");
  } catch (error) {
    setError(error.message);
  }
}

onMounted(async () => {
  await bootstrap();
  window.setInterval(async () => {
    try {
      const status = await api("/api/status");
      state.status = status.status;
    } catch {
      // Ignore transient polling errors.
    }
  }, 3000);
});
</script>
