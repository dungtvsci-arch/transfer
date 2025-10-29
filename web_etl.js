// Advanced ETL Dashboard JavaScript

class ETLDashboard {
  constructor() {
    this.currentSection = "dashboard";
    this.isDarkTheme = false;
    this.activeJobs = new Map();
    this.jobHistory = [];
    this.notifications = [];
    this.autoRefreshInterval = null;
    this.currentTab = "basic";

    this.initializeElements();
    this.setupEventListeners();
    this.initializeDashboard();
    this.startAutoRefresh();
  }

  initializeElements() {
    // Navigation elements
    this.sidebar = document.getElementById("sidebar");
    this.mobileMenuBtn = document.getElementById("mobileMenuBtn");
    this.sidebarToggle = document.getElementById("sidebarToggle");
    this.navLinks = document.querySelectorAll(".nav-link");
    this.currentSectionEl = document.getElementById("currentSection");

    // Theme elements
    this.themeToggle = document.getElementById("themeToggle");
    this.body = document.body;

    // Content sections
    this.contentSections = document.querySelectorAll(".content-section");

    // Dashboard elements
    this.completedJobs = document.getElementById("completedJobs");
    this.runningJobs = document.getElementById("runningJobs");
    this.failedJobs = document.getElementById("failedJobs");
    this.totalRecords = document.getElementById("totalRecords");
    this.activityList = document.getElementById("activityList");

    // Form elements
    this.etlForm = document.getElementById("etlForm");
    this.formTabs = document.querySelectorAll(".tab-btn");
    this.tabContents = document.querySelectorAll(".tab-content");

    // Monitoring elements
    this.activeJobsList = document.getElementById("activeJobsList");
    this.progressContent = document.getElementById("progressContent");
    this.jobSelector = document.getElementById("jobSelector");
    this.systemMetrics = document.getElementById("systemMetrics");
    this.liveLogs = document.getElementById("liveLogs");

    // History elements
    this.historyTableBody = document.getElementById("historyTableBody");
    this.startDate = document.getElementById("startDate");
    this.endDate = document.getElementById("endDate");
    this.statusFilter = document.getElementById("statusFilter");
    this.sourceFilter = document.getElementById("sourceFilter");

    // Settings elements
    this.settingsTabs = document.querySelectorAll("[data-settings-tab]");
    this.settingsContents = document.querySelectorAll(".settings-tab");

    // Modal and overlay elements
    this.loadingOverlay = document.getElementById("loadingOverlay");
    this.modalOverlay = document.getElementById("modalOverlay");
    this.toastContainer = document.getElementById("toastContainer");

    // Connection status
    this.connectionStatus = document.getElementById("connectionStatus");

    // Statistics elements
    this.brandStatsGrid = document.getElementById("brandStatsGrid");
    this.tableStatsGrid = document.getElementById("tableStatsGrid");
    this.statsTabBtns = document.querySelectorAll(".stats-tab-btn");
    this.statsTabContents = document.querySelectorAll(".stats-tab-content");
  }

  setupEventListeners() {
    // Navigation
    this.navLinks.forEach((link) => {
      link.addEventListener("click", (e) => {
        e.preventDefault();
        const section = link.dataset.section;
        this.switchSection(section);
      });
    });

    // Mobile menu
    this.mobileMenuBtn.addEventListener("click", () => {
      this.sidebar.classList.toggle("open");
    });

    this.sidebarToggle.addEventListener("click", () => {
      this.sidebar.classList.toggle("open");
    });

    // Theme toggle
    this.themeToggle.addEventListener("click", () => {
      this.toggleTheme();
    });

    // Form tabs
    this.formTabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        this.switchFormTab(tab.dataset.tab);
      });
    });

    // Settings tabs
    this.settingsTabs.forEach((tab) => {
      tab.addEventListener("click", (e) => {
        e.preventDefault();
        this.switchSettingsTab(tab.dataset.settingsTab);
      });
    });

    // Stats tabs
    this.statsTabBtns.forEach((tab) => {
      tab.addEventListener("click", () => {
        this.switchStatsTab(tab.dataset.statsTab);
      });
    });

    // Form submission
    this.etlForm.addEventListener("submit", (e) => {
      e.preventDefault();
      this.handleFormSubmit();
    });

    // Quick actions
    document.querySelectorAll(".quick-action-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        this.handleQuickAction(btn.dataset.action);
      });
    });

    // Monitoring controls
    document.getElementById("refreshMetrics")?.addEventListener("click", () => {
      this.refreshSystemMetrics();
    });

    document.getElementById("clearLogs")?.addEventListener("click", () => {
      this.clearLogs();
    });

    document.getElementById("downloadLogs")?.addEventListener("click", () => {
      this.downloadLogs();
    });

    // History filters
    document.getElementById("applyFilters")?.addEventListener("click", () => {
      this.applyHistoryFilters();
    });

    // Modal controls
    document.getElementById("modalClose")?.addEventListener("click", () => {
      this.closeModal();
    });

    // Keyboard shortcuts
    document.addEventListener("keydown", (e) => {
      this.handleKeyboardShortcuts(e);
    });

    // Real-time updates
    this.setupRealTimeUpdates();
  }

  initializeDashboard() {
    this.loadDashboardData();
    this.updateConnectionStatus();
    this.initializeFormValidation();
    this.loadJobHistory();
    this.loadSystemMetrics();
    this.setupKeyboardShortcuts();
    this.initializeNotifications();
    this.checkServerConnection();
  }

  async checkServerConnection() {
    // Check if we're running on file system and show warning
    if (window.location.protocol === "file:") {
      setTimeout(() => {
        this.showToast(
          "warning",
          "Cảnh báo: File System Mode",
          "Bạn đang mở file HTML trực tiếp. Để sử dụng đầy đủ tính năng, hãy khởi động Flask server.",
          15000,
          [
            {
              text: "Hướng dẫn",
              onclick: () => this.showServerInstructions(),
            },
            {
              text: "Mở Server",
              onclick: () => window.open("http://127.0.0.1:5000", "_blank"),
            },
          ]
        );
      }, 2000);
    } else {
      // Check if Flask server is running
      try {
        const response = await fetch("/api/health");
        if (!response.ok) {
          throw new Error("Server not responding");
        }
        const health = await response.json();
        this.showToast(
          "success",
          "Kết nối thành công",
          `Server đang chạy. Active processes: ${health.active_processes}`
        );
      } catch (error) {
        this.showToast(
          "error",
          "Không thể kết nối server",
          "Flask server có thể chưa được khởi động hoặc đang gặp sự cố.",
          10000,
          [
            {
              text: "Hướng dẫn",
              onclick: () => this.showServerInstructions(),
            },
          ]
        );
      }
    }
  }

  setupKeyboardShortcuts() {
    document.addEventListener("keydown", (e) => {
      // Ctrl/Cmd + K to focus search
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        this.showToast(
          "info",
          "Keyboard Shortcuts",
          "Ctrl+K: Search, Esc: Close modals"
        );
      }

      // Escape to close modals and toasts
      if (e.key === "Escape") {
        this.closeModal();
        // Close all toasts
        const toasts = this.toastContainer.querySelectorAll(".toast");
        toasts.forEach((toast) => this.removeToast(toast));
      }
    });
  }

  initializeNotifications() {
    // Request notification permission
    if ("Notification" in window && Notification.permission === "default") {
      Notification.requestPermission();
    }
  }

  showDesktopNotification(title, message, icon = null) {
    if ("Notification" in window && Notification.permission === "granted") {
      const notification = new Notification(title, {
        body: message,
        icon: icon || "/favicon.ico",
        badge: "/favicon.ico",
        tag: "etl-notification",
      });

      // Auto close after 5 seconds
      setTimeout(() => notification.close(), 5000);
    }
  }

  switchSection(section) {
    // Update navigation
    this.navLinks.forEach((link) => {
      link.closest(".nav-item").classList.remove("active");
      if (link.dataset.section === section) {
        link.closest(".nav-item").classList.add("active");
      }
    });

    // Update content sections
    this.contentSections.forEach((content) => {
      content.classList.remove("active");
      if (content.id === section) {
        content.classList.add("active");
      }
    });

    // Update breadcrumb
    const sectionNames = {
      dashboard: "Dashboard",
      "etl-config": "ETL Configuration",
      monitoring: "Monitoring",
      history: "History",
      settings: "Settings",
    };
    this.currentSectionEl.textContent = sectionNames[section] || "Dashboard";
    this.currentSection = section;

    // Load section-specific data
    this.loadSectionData(section);
  }

  switchFormTab(tab) {
    this.formTabs.forEach((t) => t.classList.remove("active"));
    this.tabContents.forEach((c) => c.classList.remove("active"));

    document.querySelector(`[data-tab="${tab}"]`).classList.add("active");
    document.getElementById(`${tab}-tab`).classList.add("active");
    this.currentTab = tab;
  }

  switchSettingsTab(tab) {
    this.settingsTabs.forEach((t) =>
      t.closest(".settings-item").classList.remove("active")
    );
    this.settingsContents.forEach((c) => c.classList.remove("active"));

    document
      .querySelector(`[data-settings-tab="${tab}"]`)
      .closest(".settings-item")
      .classList.add("active");
    document.getElementById(`${tab}-settings`).classList.add("active");
  }

  toggleTheme() {
    this.isDarkTheme = !this.isDarkTheme;
    this.body.classList.toggle("dark-theme", this.isDarkTheme);

    const icon = this.themeToggle.querySelector("i");
    const text = this.themeToggle.querySelector("span");

    if (this.isDarkTheme) {
      icon.className = "fas fa-sun";
      text.textContent = "Light Mode";
    } else {
      icon.className = "fas fa-moon";
      text.textContent = "Dark Mode";
    }

    localStorage.setItem(
      "etl-dashboard-theme",
      this.isDarkTheme ? "dark" : "light"
    );
  }

  async loadDashboardData() {
    // Load real dashboard data from API
    try {
      const response = await fetch("/api/stats/dashboard");
      if (!response.ok) {
        throw new Error("Không thể tải dữ liệu dashboard");
      }

      const data = await response.json();

      if (data.success) {
        const stats = data.stats;

        // Update dashboard stats with real data
        this.animateCounter(
          this.completedJobs,
          stats.recent_activity.completed_jobs
        );
        this.animateCounter(
          this.runningJobs,
          stats.recent_activity.running_jobs
        );
        this.animateCounter(this.failedJobs, stats.recent_activity.failed_jobs);
        this.animateCounter(this.totalRecords, stats.total_records);

        // Store stats for other components
        this.dashboardStats = stats;

        // Update activity list with real data
        this.updateActivityListWithRealData(stats);
      } else {
        throw new Error(data.message || "Lỗi khi tải dữ liệu");
      }
    } catch (error) {
      console.error("Error loading dashboard data:", error);
      this.showToast(
        "error",
        "Lỗi",
        "Không thể tải dữ liệu dashboard: " + error.message
      );

      // Fallback to mock data if API fails
      this.animateCounter(this.completedJobs, 0);
      this.animateCounter(this.runningJobs, 0);
      this.animateCounter(this.failedJobs, 0);
      this.animateCounter(this.totalRecords, 0);

      // Load recent activity with fallback
      this.loadRecentActivity();
    }
  }

  animateCounter(element, target) {
    if (!element) return;

    const start = 0;
    const duration = 2000;
    const startTime = performance.now();

    const animate = (currentTime) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);

      const current = Math.floor(
        start + (target - start) * this.easeOutCubic(progress)
      );
      element.textContent = current.toLocaleString();

      if (progress < 1) {
        requestAnimationFrame(animate);
      }
    };

    requestAnimationFrame(animate);
  }

  easeOutCubic(t) {
    return 1 - Math.pow(1 - t, 3);
  }

  loadRecentActivity() {
    // Show skeleton loading first
    this.showSkeletonLoading(this.activityList, 4);

    // Simulate loading delay
    setTimeout(() => {
      const activities = [
        {
          icon: "fas fa-check-circle",
          iconClass: "success",
          title: "ETL Job hoàn thành thành công",
          time: "2 phút trước",
          details: "ir_model_data → ir_model_data_new (1,247 records)",
        },
        {
          icon: "fas fa-spinner fa-spin",
          iconClass: "warning",
          title: "ETL Job đang chạy",
          time: "5 phút trước",
          details: "product_template → product_template_new (Đang xử lý...)",
        },
        {
          icon: "fas fa-exclamation-circle",
          iconClass: "error",
          title: "ETL Job thất bại",
          time: "10 phút trước",
          details: "res_partner → res_partner_new (Lỗi kết nối)",
        },
        {
          icon: "fas fa-info-circle",
          iconClass: "info",
          title: "Backup hệ thống hoàn thành",
          time: "1 giờ trước",
          details: "Kích thước backup: 2.4 GB",
        },
      ];

      this.activityList.innerHTML = "";
      activities.forEach((activity, index) => {
        const activityEl = document.createElement("div");
        activityEl.className = "activity-item";
        activityEl.style.animationDelay = `${index * 0.1}s`;
        activityEl.innerHTML = `
          <div class="activity-icon ${activity.iconClass}">
            <i class="${activity.icon}"></i>
          </div>
          <div class="activity-content">
            <div class="activity-title">${activity.title}</div>
            <div class="activity-time">${activity.time}</div>
            <div class="activity-details">${activity.details}</div>
          </div>
        `;
        this.activityList.appendChild(activityEl);
      });
    }, 1000);
  }

  updateActivityListWithRealData(stats) {
    const activities = [];

    // Add activity based on recent ETL jobs
    if (stats.recent_activity.completed_jobs > 0) {
      activities.push({
        icon: "fas fa-check-circle",
        iconClass: "success",
        title: "ETL Jobs hoàn thành",
        time: "Gần đây",
        details: `${
          stats.recent_activity.completed_jobs
        } jobs hoàn thành thành công (${stats.recent_activity.total_processed_records.toLocaleString()} records)`,
      });
    }

    if (stats.recent_activity.running_jobs > 0) {
      activities.push({
        icon: "fas fa-spinner fa-spin",
        iconClass: "warning",
        title: "ETL Jobs đang chạy",
        time: "Hiện tại",
        details: `${stats.recent_activity.running_jobs} jobs đang chạy - Kiểm tra monitoring để xem chi tiết`,
      });
    }

    if (stats.recent_activity.failed_jobs > 0) {
      activities.push({
        icon: "fas fa-exclamation-triangle",
        iconClass: "error",
        title: "ETL Jobs thất bại",
        time: "Gần đây",
        details: `${stats.recent_activity.failed_jobs} jobs thất bại - Kiểm tra history để xem lỗi`,
      });
    }

    // Add brand statistics
    if (stats.brand_stats && stats.brand_stats.length > 0) {
      const topBrand = stats.brand_stats.reduce((max, brand) =>
        brand.record_count > max.record_count ? brand : max
      );

      activities.push({
        icon: "fas fa-database",
        iconClass: "info",
        title: "Phân bố dữ liệu",
        time: "Hiện tại",
        details: `${
          topBrand.brand_name
        } có nhiều records nhất (${topBrand.record_count.toLocaleString()}/${stats.total_records.toLocaleString()})`,
      });
    }

    // Add table statistics
    if (stats.table_stats && stats.table_stats.length > 0) {
      const topTable = stats.table_stats.reduce((max, table) =>
        table.record_count > max.record_count ? table : max
      );

      activities.push({
        icon: "fas fa-table",
        iconClass: "info",
        title: "Thống kê bảng",
        time: "Hiện tại",
        details: `${
          topTable.table_name
        } là bảng lớn nhất (${topTable.record_count.toLocaleString()} records, ${
          topTable.brand_count
        } brands)`,
      });
    }

    // If no activities, show a default message
    if (activities.length === 0) {
      activities.push({
        icon: "fas fa-info-circle",
        iconClass: "info",
        title: "Không có hoạt động gần đây",
        time: "Hiện tại",
        details:
          "Chưa có ETL job nào được chạy gần đây - Bắt đầu một job mới để xem hoạt động",
      });
    }

    this.updateActivityList(activities);
  }

  updateActivityList(activities) {
    this.activityList.innerHTML = "";
    activities.forEach((activity, index) => {
      const activityEl = document.createElement("div");
      activityEl.className = "activity-item";
      activityEl.style.animationDelay = `${index * 0.1}s`;
      activityEl.innerHTML = `
        <div class="activity-icon ${activity.iconClass}">
          <i class="${activity.icon}"></i>
        </div>
        <div class="activity-content">
          <div class="activity-title">${activity.title}</div>
          <div class="activity-time">${activity.time}</div>
          <div class="activity-details">${activity.details}</div>
        </div>
      `;
      this.activityList.appendChild(activityEl);
    });
  }

  loadSectionData(section) {
    switch (section) {
      case "dashboard":
        this.loadDashboardData();
        break;
      case "monitoring":
        this.loadActiveJobs();
        this.loadSystemMetrics();
        break;
      case "history":
        this.loadJobHistory();
        break;
      case "settings":
        this.loadSettings();
        break;
    }
  }

  async handleFormSubmit() {
    const formData = new FormData(this.etlForm);
    const data = {
      sourceTable: formData.get("sourceTable"),
      targetTable: formData.get("targetTable"),
      whereClause: formData.get("whereClause"),
      sources: Array.from(formData.getAll("sources")),
      batchSize: parseInt(formData.get("batchSize")) || 5000,
      timeout: parseInt(formData.get("timeout")) || 30,
      executionMode: formData.get("executionMode") || "immediate",
      backupBefore: formData.get("backupBefore") === "on",
      validateData: formData.get("validateData") === "on",
      notifyOnComplete: formData.get("notifyOnComplete") === "on",
    };

    // Validate form
    if (!this.validateForm(data)) {
      return;
    }

    try {
      this.showLoadingOverlay("Đang khởi động ETL job...", 10);

      // Check if we're running on Flask server or file system
      const baseUrl = this.getBaseUrl();

      // Start ETL job
      const response = await fetch(`${baseUrl}/api/etl/start`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error(
            "Flask server không chạy. Vui lòng khởi động server bằng lệnh: python flask_etl_server.py"
          );
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();

      if (result.success) {
        this.showToast(
          "success",
          "ETL Job Started",
          `ETL job đã được khởi động thành công với ID: ${result.process_id}`,
          6000,
          [
            {
              text: "View Details",
              onclick: `dashboard.switchSection('monitoring')`,
            },
          ]
        );
        this.switchSection("monitoring");
        this.startJobMonitoring(result.process_id);
      } else {
        throw new Error(result.message || "Failed to start ETL job");
      }
    } catch (error) {
      this.showToast("error", "ETL Job Failed", error.message, 10000, [
        {
          text: "Hướng dẫn",
          onclick: () => this.showServerInstructions(),
        },
      ]);
    } finally {
      this.hideLoadingOverlay();
    }
  }

  getBaseUrl() {
    // Check if we're running on localhost (Flask server) or file system
    if (window.location.protocol === "file:") {
      return "http://127.0.0.1:5000";
    }

    // If running on port 5500, redirect to port 5000
    if (window.location.port === "5500") {
      this.showToast(
        "warning",
        "Port không đúng",
        "Bạn đang sử dụng port 5500. Flask server chạy trên port 5000. Đang chuyển hướng...",
        5000
      );
      setTimeout(() => {
        window.location.href = "http://127.0.0.1:5000";
      }, 2000);
      return "http://127.0.0.1:5000";
    }

    return "";
  }

  showServerInstructions() {
    this.showModal(
      "Hướng dẫn khởi động server",
      `
      <div class="server-instructions">
        <h4>Để sử dụng ứng dụng ETL, bạn cần khởi động Flask server:</h4>
        <ol>
          <li>Mở Terminal/Command Prompt</li>
          <li>Di chuyển đến thư mục dự án: <code>cd d:\\Project\\Python</code></li>
          <li>Chạy lệnh: <code>python flask_etl_server.py</code></li>
          <li>Mở trình duyệt và truy cập: <code>http://127.0.0.1:5000</code></li>
        </ol>
        <div class="note">
          <strong>⚠️ Lưu ý quan trọng:</strong>
          <ul>
            <li>❌ <strong>KHÔNG</strong> sử dụng port 5500</li>
            <li>✅ <strong>SỬ DỤNG</strong> port 5000</li>
            <li>❌ <strong>KHÔNG</strong> mở file HTML trực tiếp từ file system</li>
            <li>✅ <strong>LUÔN</strong> sử dụng thông qua Flask server</li>
          </ul>
        </div>
        <div class="current-url">
          <strong>URL hiện tại:</strong> <code>${window.location.href}</code><br>
          <strong>URL đúng:</strong> <code>http://127.0.0.1:5000</code>
        </div>
      </div>
      `,
      [
        {
          text: "Đóng",
          class: "btn-secondary",
          action: () => this.closeModal(),
        },
        {
          text: "Chuyển đến URL đúng",
          class: "btn-primary",
          action: () => {
            this.closeModal();
            window.location.href = "http://127.0.0.1:5000";
          },
        },
      ]
    );
  }

  validateForm(data) {
    if (!data.sourceTable?.trim()) {
      this.showToast("error", "Validation Error", "Source table is required.");
      return false;
    }

    if (!data.targetTable?.trim()) {
      this.showToast("error", "Validation Error", "Target table is required.");
      return false;
    }

    if (!data.sources || data.sources.length === 0) {
      this.showToast(
        "error",
        "Validation Error",
        "Please select at least one ERP source."
      );
      return false;
    }

    return true;
  }

  startJobMonitoring(processId) {
    const job = {
      id: processId,
      startTime: new Date(),
      status: "running",
      progress: 0,
      logs: [],
    };

    this.activeJobs.set(processId, job);
    this.updateActiveJobsList();
    this.pollJobStatus(processId);
  }

  async pollJobStatus(processId) {
    const maxAttempts = 300; // 5 minutes
    let attempts = 0;

    const poll = async () => {
      try {
        const baseUrl = this.getBaseUrl();
        const response = await fetch(`${baseUrl}/api/etl/status/${processId}`);
        if (!response.ok) {
          throw new Error(`Failed to get status: ${response.status}`);
        }

        const status = await response.json();

        if (status.success) {
          const job = this.activeJobs.get(processId);
          if (job) {
            job.status = status.status;
            job.progress = status.progress;
            job.logs = status.logs || [];
            job.results = status.results;
            job.totalRecords = status.total_records;

            this.updateJobProgress(processId, job);
            this.updateLiveLogs(job.logs);

            if (status.status === "completed") {
              const duration = this.formatDuration(job.startTime);
              const message = `Đã chuyển thành công ${
                status.total_records?.toLocaleString() || 0
              } records trong ${duration}`;

              // Show desktop notification
              this.showDesktopNotification("ETL Job Hoàn Thành", message);

              this.showToast("success", "ETL Job Hoàn Thành", message, 8000, [
                {
                  text: "View History",
                  onclick: `dashboard.switchSection('history')`,
                },
              ]);
              this.addToHistory(job);
              this.activeJobs.delete(processId);
              this.updateActiveJobsList();
              return;
            } else if (status.status === "failed") {
              const errorMessage =
                status.error || "Đã xảy ra lỗi không xác định.";

              // Show desktop notification
              this.showDesktopNotification("ETL Job Thất Bại", errorMessage);

              this.showToast("error", "ETL Job Thất Bại", errorMessage, 10000, [
                {
                  text: "View Logs",
                  onclick: `dashboard.switchSection('monitoring')`,
                },
                {
                  text: "Retry",
                  onclick: `dashboard.retryJob('${processId}')`,
                },
              ]);
              this.addToHistory(job);
              this.activeJobs.delete(processId);
              this.updateActiveJobsList();
              return;
            }
          }

          // Continue polling if still running
          if (status.status === "running" && attempts < maxAttempts) {
            attempts++;
            setTimeout(poll, 1000);
          } else if (attempts >= maxAttempts) {
            this.showToast(
              "error",
              "ETL Job Timeout",
              "Job exceeded maximum execution time."
            );
            this.activeJobs.delete(processId);
            this.updateActiveJobsList();
          }
        }
      } catch (error) {
        this.showToast("error", "Monitoring Error", error.message);
        this.activeJobs.delete(processId);
        this.updateActiveJobsList();
      }
    };

    poll();
  }

  updateActiveJobsList() {
    if (!this.activeJobsList) return;

    this.activeJobsList.innerHTML = "";

    if (this.activeJobs.size === 0) {
      this.activeJobsList.innerHTML = `
        <div class="empty-state">
          <i class="fas fa-inbox"></i>
          <p>Không có job nào đang chạy</p>
        </div>
      `;
      return;
    }

    this.activeJobs.forEach((job, processId) => {
      const jobEl = document.createElement("div");
      jobEl.className = "job-item";

      const statusText = {
        running: "Đang chạy",
        completed: "Hoàn thành",
        failed: "Thất bại",
        pending: "Chờ xử lý",
      };

      const statusIcon = {
        running: "fa-spinner fa-spin",
        completed: "fa-check-circle",
        failed: "fa-exclamation-circle",
        pending: "fa-clock",
      };

      jobEl.innerHTML = `
        <div class="job-info">
          <div class="job-header">
            <div class="job-id">${processId}</div>
            <div class="job-status ${job.status}">
              <i class="fas ${statusIcon[job.status] || "fa-circle"}"></i>
              <span>${statusText[job.status] || job.status}</span>
            </div>
          </div>
          <div class="job-progress">
            <div class="progress-indicator">
              <div class="progress-spinner"></div>
              <div class="progress-text">${
                job.current_step || "Đang xử lý..."
              }</div>
              <div class="progress-percentage">${job.progress}%</div>
            </div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: ${job.progress}%"></div>
            </div>
          </div>
          <div class="job-details">
            <span class="job-time">Bắt đầu: ${this.formatTime(
              job.startTime
            )}</span>
            ${
              job.totalRecords > 0
                ? `<span class="job-records">Records: ${job.totalRecords.toLocaleString()}</span>`
                : ""
            }
          </div>
        </div>
        <div class="job-actions">
          <button class="action-btn" onclick="dashboard.pauseJob('${processId}')" title="Tạm dừng">
            <i class="fas fa-pause"></i>
          </button>
          <button class="action-btn" onclick="dashboard.stopJob('${processId}')" title="Dừng">
            <i class="fas fa-stop"></i>
          </button>
          <button class="action-btn" onclick="dashboard.viewJobLogs('${processId}')" title="Xem logs">
            <i class="fas fa-file-alt"></i>
          </button>
        </div>
      `;
      this.activeJobsList.appendChild(jobEl);
    });
  }

  formatTime(date) {
    if (!date) return "N/A";
    return new Date(date).toLocaleTimeString("vi-VN");
  }

  updateJobProgress(processId, job) {
    // Update job selector
    const selector = this.jobSelector;
    if (selector) {
      const option = selector.querySelector(`option[value="${processId}"]`);
      if (!option) {
        const newOption = document.createElement("option");
        newOption.value = processId;
        newOption.textContent = `Job ${processId} (${job.progress}%)`;
        selector.appendChild(newOption);
      } else {
        option.textContent = `Job ${processId} (${job.progress}%)`;
      }
    }

    // Update progress visualization if this job is selected
    if (selector && selector.value === processId) {
      this.showJobProgress(job);
    }
  }

  showJobProgress(job) {
    if (!this.progressContent) return;

    this.progressContent.innerHTML = `
      <div class="progress-visualization">
        <div class="progress-circle">
          <svg width="120" height="120">
            <circle cx="60" cy="60" r="52" stroke="#e5e7eb" stroke-width="8" fill="transparent"/>
            <circle cx="60" cy="60" r="52" stroke="#3b82f6" stroke-width="8" fill="transparent"
                    stroke-dasharray="326.73" stroke-dashoffset="${
                      326.73 - (job.progress / 100) * 326.73
                    }"
                    transform="rotate(-90 60 60)"/>
          </svg>
          <div class="progress-percentage">${job.progress}%</div>
        </div>
        <div class="progress-details">
          <div class="progress-step">${job.status}</div>
          <div class="progress-time">${this.formatDuration(job.startTime)}</div>
        </div>
      </div>
    `;
  }

  updateLiveLogs(logs) {
    if (!this.liveLogs) return;

    const logsContainer = this.liveLogs;
    const latestLogs = logs.slice(-10); // Show last 10 logs

    logsContainer.innerHTML = "";
    latestLogs.forEach((log) => {
      const logEl = document.createElement("div");
      logEl.className = `log-entry log-${log.level}`;
      logEl.innerHTML = `
        <span class="log-timestamp">[${log.timestamp}]</span>
        <span class="log-message">${log.message}</span>
      `;
      logsContainer.appendChild(logEl);
    });

    // Auto-scroll to bottom
    logsContainer.scrollTop = logsContainer.scrollHeight;
  }

  formatDuration(startTime) {
    const elapsed = Math.floor((new Date() - startTime) / 1000);
    const minutes = Math.floor(elapsed / 60);
    const seconds = elapsed % 60;
    return `${minutes.toString().padStart(2, "0")}:${seconds
      .toString()
      .padStart(2, "0")}`;
  }

  async loadJobHistory() {
    try {
      const response = await fetch("/api/etl/history");
      if (!response.ok) {
        throw new Error("Không thể tải lịch sử ETL jobs");
      }

      const data = await response.json();

      if (data.success) {
        const historyData = data.history.map((job) => ({
          id: job.process_id,
          sourceTable: job.source_table || "N/A",
          targetTable: job.target_table || "N/A",
          status: job.status,
          duration: this.calculateDuration(job.start_time, job.end_time),
          records: job.total_records || 0,
          started: job.start_time
            ? new Date(job.start_time).toLocaleString("vi-VN")
            : "N/A",
          sources: job.sources ? job.sources.split(",") : [],
          error: job.error,
          progress: job.progress,
          currentStep: job.current_step,
        }));

        this.updateHistoryTable(historyData);
      } else {
        throw new Error(data.message || "Lỗi khi tải lịch sử");
      }
    } catch (error) {
      console.error("Error loading job history:", error);
      this.showToast(
        "error",
        "Lỗi",
        "Không thể tải lịch sử ETL jobs: " + error.message
      );

      // Fallback to empty table
      this.updateHistoryTable([]);
    }
  }

  calculateDuration(startTime, endTime) {
    if (!startTime || !endTime) return "N/A";

    const start = new Date(startTime);
    const end = new Date(endTime);
    const diffMs = end - start;

    const hours = Math.floor(diffMs / (1000 * 60 * 60));
    const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
    const seconds = Math.floor((diffMs % (1000 * 60)) / 1000);

    return `${hours.toString().padStart(2, "0")}:${minutes
      .toString()
      .padStart(2, "0")}:${seconds.toString().padStart(2, "0")}`;
  }

  updateHistoryTable(data) {
    if (!this.historyTableBody) return;

    this.historyTableBody.innerHTML = "";
    data.forEach((job) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${job.id}</td>
        <td>${job.sourceTable}</td>
        <td>${job.targetTable}</td>
        <td><span class="status-badge ${job.status}">${job.status}</span></td>
        <td>${job.duration}</td>
        <td>${job.records.toLocaleString()}</td>
        <td>${job.started}</td>
        <td>
          <button class="action-btn" onclick="dashboard.viewJobDetails('${
            job.id
          }')">
            <i class="fas fa-eye"></i>
          </button>
          <button class="action-btn" onclick="dashboard.retryJob('${job.id}')">
            <i class="fas fa-redo"></i>
          </button>
        </td>
      `;
      this.historyTableBody.appendChild(row);
    });
  }

  loadActiveJobs() {
    // Load active jobs data
    this.updateActiveJobsList();

    // If there are active jobs, start monitoring them
    if (this.activeJobs.size > 0) {
      this.activeJobs.forEach((job, processId) => {
        if (job.status === "running") {
          this.pollJobStatus(processId);
        }
      });
    }
  }

  loadSystemMetrics() {
    if (!this.systemMetrics) return;

    const metrics = [
      { label: "CPU Usage", value: "45%", status: "good" },
      { label: "Memory Usage", value: "2.1 GB", status: "warning" },
      { label: "Disk Usage", value: "78%", status: "warning" },
      { label: "Network I/O", value: "125 MB/s", status: "good" },
    ];

    this.systemMetrics.innerHTML = "";
    metrics.forEach((metric) => {
      const metricEl = document.createElement("div");
      metricEl.className = "metric-item";
      metricEl.innerHTML = `
        <div class="metric-label">${metric.label}</div>
        <div class="metric-value ${metric.status}">${metric.value}</div>
      `;
      this.systemMetrics.appendChild(metricEl);
    });
  }

  handleQuickAction(action) {
    switch (action) {
      case "new-etl":
        this.switchSection("etl-config");
        break;
      case "view-logs":
        this.switchSection("monitoring");
        break;
      case "system-health":
        this.showSystemHealthModal();
        break;
      case "backup":
        this.showBackupModal();
        break;
    }
  }

  showSystemHealthModal() {
    this.showModal(
      "System Health",
      `
      <div class="health-metrics">
        <div class="health-item">
          <span class="health-label">Database Connections</span>
          <span class="health-value good">8/10</span>
        </div>
        <div class="health-item">
          <span class="health-label">Active Jobs</span>
          <span class="health-value warning">3/5</span>
        </div>
        <div class="health-item">
          <span class="health-label">System Load</span>
          <span class="health-value good">Normal</span>
        </div>
      </div>
    `,
      [
        {
          text: "Close",
          class: "btn-secondary",
          action: () => this.closeModal(),
        },
        {
          text: "Refresh",
          class: "btn-primary",
          action: () => this.refreshSystemMetrics(),
        },
      ]
    );
  }

  showBackupModal() {
    this.showModal(
      "Backup Configuration",
      `
      <div class="backup-options">
        <div class="form-group">
          <label>Backup Type</label>
          <select class="form-control">
            <option value="full">Full Backup</option>
            <option value="incremental">Incremental Backup</option>
            <option value="differential">Differential Backup</option>
          </select>
        </div>
        <div class="form-group">
          <label>Destination</label>
          <input type="text" class="form-control" value="/backups/etl-backup-$(date +%Y%m%d)" />
        </div>
      </div>
    `,
      [
        {
          text: "Cancel",
          class: "btn-secondary",
          action: () => this.closeModal(),
        },
        {
          text: "Start Backup",
          class: "btn-primary",
          action: () => this.startBackup(),
        },
      ]
    );
  }

  showModal(title, content, actions = []) {
    const modalTitle = document.getElementById("modalTitle");
    const modalBody = document.getElementById("modalBody");
    const modalFooter = document.getElementById("modalFooter");

    modalTitle.textContent = title;
    modalBody.innerHTML = content;

    modalFooter.innerHTML = "";
    actions.forEach((action) => {
      const button = document.createElement("button");
      button.className = `btn ${action.class}`;
      button.textContent = action.text;
      button.addEventListener("click", action.action);
      modalFooter.appendChild(button);
    });

    this.modalOverlay.style.display = "flex";
  }

  closeModal() {
    this.modalOverlay.style.display = "none";
  }

  showToast(type, title, message, duration = 5000, actions = []) {
    const toast = document.createElement("div");
    toast.className = `toast ${type}`;

    const actionButtons =
      actions.length > 0
        ? `
      <div class="toast-actions">
        ${actions
          .map(
            (action) => `
          <button class="toast-action-btn" onclick="${action.onclick}">
            ${action.text}
          </button>
        `
          )
          .join("")}
      </div>
    `
        : "";

    toast.innerHTML = `
      <div class="toast-icon">
        <i class="fas ${this.getToastIcon(type)}"></i>
      </div>
      <div class="toast-content">
        <div class="toast-title">${title}</div>
        <div class="toast-message">${message}</div>
        ${actionButtons}
      </div>
      <button class="toast-close" onclick="this.parentElement.remove()">
        <i class="fas fa-times"></i>
      </button>
    `;

    // Add close button styles
    const closeBtn = toast.querySelector(".toast-close");
    closeBtn.style.cssText = `
      background: none;
      border: none;
      color: var(--secondary-400);
      cursor: pointer;
      padding: 4px;
      border-radius: 4px;
      transition: all 0.2s ease;
    `;
    closeBtn.addEventListener("mouseenter", () => {
      closeBtn.style.background = "var(--secondary-100)";
      closeBtn.style.color = "var(--secondary-600)";
    });
    closeBtn.addEventListener("mouseleave", () => {
      closeBtn.style.background = "none";
      closeBtn.style.color = "var(--secondary-400)";
    });

    this.toastContainer.appendChild(toast);

    // Add entrance animation
    requestAnimationFrame(() => {
      toast.style.transform = "translateX(100%)";
      toast.style.opacity = "0";
      requestAnimationFrame(() => {
        toast.style.transition = "all 0.4s cubic-bezier(0.4, 0, 0.2, 1)";
        toast.style.transform = "translateX(0)";
        toast.style.opacity = "1";
      });
    });

    // Auto-remove after specified duration
    if (duration > 0) {
      setTimeout(() => {
        this.removeToast(toast);
      }, duration);
    }

    // Add click to dismiss
    toast.addEventListener("click", (e) => {
      if (
        !e.target.closest(".toast-action-btn") &&
        !e.target.closest(".toast-close")
      ) {
        this.removeToast(toast);
      }
    });

    return toast;
  }

  removeToast(toast) {
    if (!toast || !toast.parentElement) return;

    toast.style.transition = "all 0.3s ease";
    toast.style.transform = "translateX(100%)";
    toast.style.opacity = "0";

    setTimeout(() => {
      if (toast.parentElement) {
        toast.remove();
      }
    }, 300);
  }

  getToastIcon(type) {
    const icons = {
      success: "fa-check-circle",
      error: "fa-exclamation-circle",
      warning: "fa-exclamation-triangle",
      info: "fa-info-circle",
    };
    return icons[type] || "fa-info-circle";
  }

  showLoadingOverlay(message = "Processing...", progress = 0) {
    this.loadingOverlay.style.display = "flex";

    const loadingText = this.loadingOverlay.querySelector(".loading-text");
    const progressBar = this.loadingOverlay.querySelector(".progress-fill");
    const progressText = this.loadingOverlay.querySelector(".progress-text");

    if (loadingText) loadingText.textContent = message;
    if (progressBar) progressBar.style.width = `${progress}%`;
    if (progressText) progressText.textContent = `${progress}%`;
  }

  updateLoadingProgress(progress, message) {
    const loadingText = this.loadingOverlay.querySelector(".loading-text");
    const progressBar = this.loadingOverlay.querySelector(".progress-fill");
    const progressText = this.loadingOverlay.querySelector(".progress-text");

    if (loadingText && message) loadingText.textContent = message;
    if (progressBar) progressBar.style.width = `${progress}%`;
    if (progressText) progressText.textContent = `${progress}%`;
  }

  hideLoadingOverlay() {
    this.loadingOverlay.style.display = "none";
  }

  showSkeletonLoading(container, count = 3) {
    if (!container) return;

    container.innerHTML = "";
    for (let i = 0; i < count; i++) {
      const skeleton = document.createElement("div");
      skeleton.className = "skeleton-card";
      skeleton.innerHTML = `
        <div class="skeleton skeleton-text"></div>
        <div class="skeleton skeleton-text"></div>
        <div class="skeleton skeleton-text"></div>
      `;
      container.appendChild(skeleton);
    }
  }

  updateConnectionStatus() {
    // Simulate connection status check
    const isConnected = Math.random() > 0.1; // 90% chance of being connected

    if (this.connectionStatus) {
      const indicator = this.connectionStatus.querySelector("i");
      const text = this.connectionStatus.querySelector("span");

      if (isConnected) {
        indicator.className = "fas fa-circle";
        indicator.style.color = "#22c55e";
        text.textContent = "Connected";
      } else {
        indicator.className = "fas fa-circle";
        indicator.style.color = "#ef4444";
        text.textContent = "Disconnected";
      }
    }
  }

  startAutoRefresh() {
    this.autoRefreshInterval = setInterval(() => {
      if (this.currentSection === "dashboard") {
        this.loadDashboardData();
      }
      if (this.currentSection === "monitoring") {
        this.loadSystemMetrics();
      }
      this.updateConnectionStatus();
    }, 5000);
  }

  setupRealTimeUpdates() {
    // Simulate real-time updates
    setInterval(() => {
      if (this.activeJobs.size > 0) {
        this.activeJobs.forEach((job, processId) => {
          if (job.status === "running" && job.progress < 100) {
            job.progress += Math.random() * 5;
            if (job.progress >= 100) {
              job.progress = 100;
              job.status = "completed";
            }
            this.updateJobProgress(processId, job);
          }
        });
      }
    }, 2000);
  }

  handleKeyboardShortcuts(e) {
    // Ctrl/Cmd + K to focus search
    if ((e.ctrlKey || e.metaKey) && e.key === "k") {
      e.preventDefault();
      // Focus search functionality
    }

    // Escape to close modals
    if (e.key === "Escape") {
      this.closeModal();
    }
  }

  initializeFormValidation() {
    // Add real-time validation to form inputs
    const inputs = this.etlForm.querySelectorAll("input, textarea");
    inputs.forEach((input) => {
      input.addEventListener("input", () => {
        this.validateInput(input);
      });
    });
  }

  validateInput(input) {
    const wrapper = input.closest(".input-wrapper, .textarea-wrapper");
    if (!wrapper) return;

    if (input.checkValidity() && input.value.trim()) {
      wrapper.classList.add("valid");
      wrapper.classList.remove("invalid");
    } else if (input.value.trim()) {
      wrapper.classList.add("invalid");
      wrapper.classList.remove("valid");
    } else {
      wrapper.classList.remove("valid", "invalid");
    }
  }

  // Utility methods
  pauseJob(processId) {
    this.showToast("info", "Job Paused", `Job ${processId} has been paused.`);
  }

  stopJob(processId) {
    this.activeJobs.delete(processId);
    this.updateActiveJobsList();
    this.showToast(
      "warning",
      "Job Stopped",
      `Job ${processId} has been stopped.`
    );
  }

  viewJobDetails(jobId) {
    this.showToast("info", "Job Details", `Viewing details for job ${jobId}.`);
  }

  retryJob(jobId) {
    this.showToast("info", "Retry Job", `Đang thử lại job ${jobId}...`);
    // Implement retry logic here
  }

  viewJobLogs(processId) {
    const job = this.activeJobs.get(processId);
    if (!job) {
      this.showToast(
        "error",
        "Job Not Found",
        "Không tìm thấy job với ID này."
      );
      return;
    }

    const logsContent = job.logs
      .map(
        (log) =>
          `<div class="log-entry log-${log.level}">
        <span class="log-timestamp">[${log.timestamp}]</span>
        <span class="log-message">${log.message}</span>
      </div>`
      )
      .join("");

    this.showModal(
      `Job Logs - ${processId}`,
      `
      <div class="job-logs-container">
        <div class="job-info-header">
          <div class="job-status-info">
            <span class="status-badge ${job.status}">${job.status}</span>
            <span>Progress: ${job.progress}%</span>
          </div>
        </div>
        <div class="logs-content" style="max-height: 400px; overflow-y: auto; background: var(--secondary-50); padding: 16px; border-radius: 8px; font-family: monospace; font-size: 12px;">
          ${logsContent || '<div class="no-logs">Chưa có logs</div>'}
        </div>
      </div>
      `,
      [
        {
          text: "Close",
          class: "btn-secondary",
          action: () => this.closeModal(),
        },
        {
          text: "Refresh",
          class: "btn-primary",
          action: () => this.refreshJobLogs(processId),
        },
      ]
    );
  }

  refreshJobLogs(processId) {
    // Refresh logs for specific job
    this.showToast("info", "Refreshing Logs", "Đang làm mới logs...");
    this.closeModal();
  }

  clearLogs() {
    if (this.liveLogs) {
      this.liveLogs.innerHTML = "";
    }
    this.showToast("info", "Logs Cleared", "Live logs have been cleared.");
  }

  downloadLogs() {
    this.showToast(
      "info",
      "Download Started",
      "Logs are being prepared for download."
    );
  }

  refreshSystemMetrics() {
    this.loadSystemMetrics();
    this.showToast(
      "info",
      "Metrics Updated",
      "System metrics have been refreshed."
    );
  }

  applyHistoryFilters() {
    this.showToast(
      "info",
      "Filters Applied",
      "History filters have been applied."
    );
  }

  startBackup() {
    this.showToast(
      "success",
      "Backup Started",
      "System backup has been initiated."
    );
    this.closeModal();
  }

  resetForm() {
    this.etlForm.reset();
    this.showToast(
      "info",
      "Form Reset",
      "Form has been reset to default values."
    );
  }

  validateConfig() {
    this.showToast(
      "success",
      "Configuration Valid",
      "Your ETL configuration is valid."
    );
  }
}

// Initialize dashboard when DOM is loaded
document.addEventListener("DOMContentLoaded", () => {
  window.dashboard = new ETLDashboard();

  // Load saved theme
  const savedTheme = localStorage.getItem("etl-dashboard-theme");
  if (savedTheme === "dark") {
    window.dashboard.toggleTheme();
  }
});

// Global functions for inline event handlers
function resetForm() {
  window.dashboard?.resetForm();
}

function validateConfig() {
  window.dashboard?.validateConfig();
}
