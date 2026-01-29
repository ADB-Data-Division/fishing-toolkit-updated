/**
 * Unified Cyclone Impact Toolkit Application
 * Handles navigation between different screens and modes
 */

class CycloneToolkitApp {
    constructor() {
        this.currentScreen = 'welcome';
        this.currentMode = null;
        this.api = null;
        this.savedDrawnTrackPath = null;
        this.statusPollInterval = null;
        this.nowcastStatusPollInterval = null;

        this.init();
    }

    init() {
        console.log('Initializing Cyclone Toolkit App...');

        // Check if running in PyWebView or browser
        this.checkEnvironment();

        this.setupEventListeners();
        this.initializeAPI();
    }

    checkEnvironment() {
        console.log('Checking environment...');
        console.log('User agent:', navigator.userAgent);
        console.log('Protocol:', window.location.protocol);
        console.log('Host:', window.location.host);

        // Check if running in PyWebView
        if (window.location.protocol === 'file:' || window.location.host === '') {
            console.log('Running in file:// mode - likely in browser');
        } else {
            console.log('Running in http:// mode - likely in PyWebView');
        }
    }

    initializeAPI() {
        console.log('Initializing API...');
        console.log('window.pywebview:', window.pywebview);
        console.log('window object keys:', Object.keys(window));

        // Wait for PyWebView API to be available
        const checkAPI = () => {
            if (window.pywebview && window.pywebview.api) {
                this.api = window.pywebview.api;
                console.log('API initialized successfully');
                console.log('API methods:', Object.keys(this.api));

                // Test the API connection
                this.testAPIConnection();
                return true;
            } else {
                console.log('Waiting for PyWebView API...');
                console.log('window.pywebview available:', !!window.pywebview);
                if (window.pywebview) {
                    console.log('window.pywebview.api available:', !!window.pywebview.api);
                }
                return false;
            }
        };

        // Try immediately first
        if (!checkAPI()) {
            // If not available, wait and retry
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds with 100ms intervals

            const retryInterval = setInterval(() => {
                attempts++;
                if (checkAPI()) {
                    clearInterval(retryInterval);
                } else if (attempts >= maxAttempts) {
                    clearInterval(retryInterval);
                    console.warn('PyWebView API not available after 5 seconds - running in browser mode');
                    console.log('Final check - window.pywebview:', window.pywebview);
                }
            }, 100);
        }
    }

    async testAPIConnection() {
        try {
            if (this.api && this.api.test_api) {
                const result = await this.api.test_api();
                console.log('API test successful:', result);
            } else {
                console.warn('API test method not available');
            }
        } catch (error) {
            console.error('API test failed:', error);
        }
    }

    setupEventListeners() {
        // Welcome screen buttons
        document.getElementById('historicalBtn').addEventListener('click', () => {
            this.showScreen('historical-config');
        });

        document.getElementById('nowcastBtn').addEventListener('click', () => {
            this.showScreen('nowcast-config');
        });

        // Back buttons
        document.getElementById('backToWelcomeBtn').addEventListener('click', () => {
            this.showScreen('welcome');
        });

        document.getElementById('backToWelcomeBtn2').addEventListener('click', () => {
            this.showScreen('welcome');
        });

        document.getElementById('backToWelcomeBtn3').addEventListener('click', () => {
            this.showScreen('welcome');
        });

        document.getElementById('backToWelcomeBtn4').addEventListener('click', () => {
            this.showScreen('welcome');
        });

        // Nowcast configuration
        document.getElementById('runNowcastBtn').addEventListener('click', () => {
            this.runNowcastAnalysis();
        });

        // Historical configuration
        document.getElementById('runHistoricalBtn').addEventListener('click', () => {
            this.runHistoricalAnalysis();
        });

        // Data source buttons for nowcast
        document.querySelectorAll('.data-source-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                // Remove active class from all buttons
                document.querySelectorAll('.data-source-btn').forEach(b => b.classList.remove('active'));
                // Add active class to clicked button
                e.target.classList.add('active');

                // Show/hide days input based on data source
                const dataSource = e.target.getAttribute('data-source');
                const ibtracsDaysGroup = document.getElementById('ibtracsDaysGroup');
                if (ibtracsDaysGroup) {
                    if (dataSource === 'ibtracs') {
                        ibtracsDaysGroup.style.display = 'block';
                    } else {
                        ibtracsDaysGroup.style.display = 'none';
                    }
                }

                // If synthetic button is clicked, open the modal
                if (dataSource === 'synthetic') {
                    console.log('Synthetic button clicked, opening modal...');
                    this.openSyntheticModal();
                }
            });
        });

        // Setup synthetic modal handlers
        this.setupSyntheticModal();

        // Data source radio buttons
        document.querySelectorAll('input[name="dataSource"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.toggleSyntheticOptions(e.target.value === 'synthetic');
            });
        });

        // File upload (only if elements exist)
        const chooseFileBtn = document.getElementById('chooseFileBtn');
        const trackFile = document.getElementById('trackFile');

        if (chooseFileBtn) {
            chooseFileBtn.addEventListener('click', () => {
                if (trackFile) {
                    trackFile.click();
                }
            });
        }

        if (trackFile) {
            trackFile.addEventListener('change', (e) => {
                if (e.target.files.length > 0 && chooseFileBtn) {
                    chooseFileBtn.textContent = `Selected: ${e.target.files[0].name}`;
                }
            });
        }

        // Error banner close button
        const closeErrorBanner = document.getElementById('closeErrorBanner');
        if (closeErrorBanner) {
            closeErrorBanner.addEventListener('click', () => {
                this.hideError();
            });
        }

    }

    showScreen(screenName) {
        console.log(`Switching to screen: ${screenName}`);

        // Hide all screens
        document.querySelectorAll('.screen').forEach(screen => {
            screen.classList.remove('active');
            console.log(`Hiding screen: ${screen.id}`);
        });

        // Show target screen
        const targetScreen = document.getElementById(`${screenName}-screen`);
        console.log(`Target screen element:`, targetScreen);
        if (targetScreen) {
            targetScreen.classList.add('active');
            this.currentScreen = screenName;
            console.log(`Successfully switched to screen: ${screenName}`);

            // Load data for specific screens
            if (screenName === 'historical-config') {
                this.loadHistoricalYears();
            } else if (screenName === 'nowcast-dashboard') {
                this.loadNowcastDashboard();
            } else if (screenName === 'historical-dashboard') {
                this.loadHistoricalDashboard();
            }
        } else {
            console.error(`Screen not found: ${screenName}`);
        }
    }

    closeApplication() {
        console.log('Close button clicked');
        // Call Python API to close the application
        if (this.api && this.api.close_app) {
            this.api.close_app().then(() => {
                console.log('Closing application...');
            }).catch((error) => {
                console.error('Error closing application:', error);
            });
        } else {
            console.warn('PyWebView API not available - cannot close application');
        }
    }

    toggleSyntheticOptions(show) {
        const syntheticOptions = document.getElementById('syntheticOptions');
        if (syntheticOptions) {
            syntheticOptions.style.display = show ? 'block' : 'none';
        }
    }

    openSyntheticModal() {
        console.log('openSyntheticModal called');
        const syntheticModal = document.getElementById('syntheticModal');
        if (syntheticModal) {
            console.log('Modal found, opening...');
            syntheticModal.classList.add('show');
        } else {
            console.error('Modal not found!');
        }
    }

    setupSyntheticModal() {
        console.log('Setting up synthetic modal...');
        const syntheticModal = document.getElementById('syntheticModal');
        const syntheticBackBtn = document.getElementById('syntheticBackBtn');
        const syntheticProceedBtn = document.getElementById('syntheticProceedBtn');
        const syntheticTabs = document.querySelectorAll('.synthetic-source-btn');
        const trackFileInput = document.getElementById('trackFileInput');
        const fileInputText = document.querySelector('.file-input-text');

        if (!syntheticModal) {
            console.error('Synthetic modal not found!');
            return;
        }

        // Close modal when Back button is clicked
        if (syntheticBackBtn) {
            syntheticBackBtn.addEventListener('click', () => {
                syntheticModal.classList.remove('show');
            });
        }

        // Close modal when clicking outside
        syntheticModal.addEventListener('click', (e) => {
            if (e.target === syntheticModal) {
                syntheticModal.classList.remove('show');
            }
        });

        // Handle tab switching (segmented control)
        syntheticTabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const tabName = tab.getAttribute('data-tab');

                // Remove active class from all tabs
                syntheticTabs.forEach(t => t.classList.remove('active'));
                // Add active class to clicked tab
                tab.classList.add('active');

                // Hide all tab panels
                document.querySelectorAll('.tab-panel').forEach(panel => {
                    panel.style.display = 'none';
                });

                // Show selected tab panel
                const selectedPanel = document.getElementById(`${tabName}Tab`);
                if (selectedPanel) {
                    selectedPanel.style.display = 'block';
                }

                // Auto-open track drawer when Draw tab is selected
                if (tabName === 'draw' && window.trackDrawer) {
                    // Close synthetic modal first
                    syntheticModal.classList.remove('show');
                    // Small delay to allow modal to close, then open track drawer
                    setTimeout(() => {
                        window.trackDrawer.open();
                    }, 100);
                }
            });
        });

        // Handle file input change
        if (trackFileInput && fileInputText) {
            trackFileInput.addEventListener('change', (e) => {
                const files = e.target.files;
                if (files && files.length > 0) {
                    const file = files[0];
                    const fileName = file.name.toLowerCase();

                    // Validate that it's a ZIP file
                    if (fileName.endsWith('.zip')) {
                        fileInputText.textContent = file.name;
                    } else {
                        // Clear the input and show error
                        trackFileInput.value = '';
                        fileInputText.textContent = 'Choose File';
                        alert('Please select a ZIP file containing a shapefile (.zip).');
                    }
                } else {
                    fileInputText.textContent = 'Choose File';
                }
            });
        }

        // Handle Proceed button - triggers Run action
        if (syntheticProceedBtn) {
            syntheticProceedBtn.addEventListener('click', () => {
                const files = trackFileInput ? trackFileInput.files : null;

                // Check for saved drawn track first
                const savedDrawnTrack = window.savedDrawnTrackPath || (window.trackDrawer ? window.trackDrawer.getSavedTrackPath() : null);

                if (files && files.length > 0) {
                    const file = files[0];
                    const fileName = file.name.toLowerCase();

                    // Validate it's a ZIP file before proceeding
                    if (!fileName.endsWith('.zip')) {
                        alert('Please select a ZIP file (.zip) containing a shapefile.');
                        return;
                    }

                    console.log('Proceeding with uploaded shapefile:', file.name);
                    // Close the modal
                    syntheticModal.classList.remove('show');
                    // Trigger the Run action
                    this.runNowcastAnalysis();
                } else if (savedDrawnTrack) {
                    // Use saved drawn track
                    console.log('Proceeding with drawn track:', savedDrawnTrack);
                    // Close the modal
                    syntheticModal.classList.remove('show');
                    // Trigger the Run action
                    this.runNowcastAnalysis();
                } else {
                    alert('Please select a shapefile or draw a track before proceeding.');
                }
            });
        }

        console.log('Synthetic modal setup complete');
    }

    async loadHistoricalYears() {
        // Generate years dynamically from 2019 to current year in descending order
        const currentYear = new Date().getFullYear();
        const startYear = 2019;
        const years = Array.from(
            { length: currentYear - startYear + 1 },
            (_, i) => currentYear - i
        );

        const yearSelect = document.getElementById('yearSelect');

        if (yearSelect) {
            yearSelect.innerHTML = '<option value="">Select Year...</option>';
            years.forEach(year => {
                const option = document.createElement('option');
                option.value = year;
                option.textContent = year;
                yearSelect.appendChild(option);
            });
        }

        // Also try to load from API if available for additional data
        if (this.api) {
            try {
                const apiYears = await this.api.get_available_years();
                console.log('API years:', apiYears);
            } catch (error) {
                console.error('Error loading years from API:', error);
            }
        }
    }

    async runNowcastAnalysis() {
        console.log('Running nowcast analysis...');

        // Get configuration
        const country = document.getElementById('countrySelect').value;
        const dataSource = document.querySelector('.data-source-btn.active').getAttribute('data-source');

        // Get days value if IBTrACS is selected
        let days = null;
        if (dataSource === 'ibtracs') {
            const daysInput = document.getElementById('ibtracsDaysInput');
            if (daysInput) {
                const daysValue = parseInt(daysInput.value, 10);
                // Validate and use default if invalid
                if (isNaN(daysValue) || daysValue < 1 || daysValue > 90) {
                    days = 7; // Default value
                    console.warn(`Invalid days value, using default: ${days}`);
                } else {
                    days = daysValue;
                }
            } else {
                days = 7; // Default if input doesn't exist
            }
        }

        console.log(`Configuration: Country=${country}, DataSource=${dataSource}, Days=${days}`);

        // Get local zip path if synthetic data source
        let localZipPath = null;
        if (dataSource === 'synthetic') {
            const trackFileInput = document.getElementById('trackFileInput');
            const files = trackFileInput ? trackFileInput.files : null;

            // Check for saved drawn track
            const savedTrackPath = window.trackDrawer ? window.trackDrawer.getSavedTrackPath() : null;

            if ((!files || files.length === 0) && !savedTrackPath) {
                alert('Please select a shapefile or draw a track before proceeding.');
                this.openSyntheticModal();
                return;
            }

            // Priority: 1) Saved drawn track, 2) Uploaded file
            if (savedTrackPath) {
                console.log('Using saved drawn track:', savedTrackPath);
                localZipPath = savedTrackPath;
            } else if (files && files.length > 0) {
                // Upload ZIP file (or shapefile) - backend will extract and return path
                try {
                    const file = files[0];
                    console.log('Uploading file:', file.name);

                    // Read file as base64
                    const fileData = await this.readFileAsBase64(file);

                    // Upload to backend - backend will extract ZIP and return shapefile path
                    if (this.api && this.api.upload_cyclone_track) {
                        const shapefilePath = await this.api.upload_cyclone_track(fileData, file.name);
                        console.log('File processed, shapefile at:', shapefilePath);
                        localZipPath = shapefilePath;
                    } else {
                        alert('API not available for file upload. Please try again.');
                        return;
                    }
                } catch (error) {
                    console.error('Error uploading file:', error);
                    alert('Failed to upload file: ' + (error.message || error));
                    return;
                }
            }
        }

        // Check if API is available
        if (!this.api || !this.api.run_nowcast_analysis) {
            this.showNowcastError('API not available. Please ensure the application is running correctly.');
            return;
        }

        // Reset loading screen
        this.resetNowcastLoadingScreen();

        // Show loading screen
        this.showScreen('nowcast-loading');

        // Setup cancel button handler
        this.setupNowcastCancelButton();

        try {
            // Start the analysis
            const result = await this.api.run_nowcast_analysis(country, null, localZipPath, days);
            console.log('Nowcast analysis started:', result);

            if (result && result.status === 'error') {
                this.showNowcastError(result.message || 'Failed to start analysis');
                return;
            }

            // Start polling for status updates
            this.startNowcastStatusPolling();

        } catch (error) {
            console.error('Error starting nowcast analysis:', error);
            this.showNowcastError(`Failed to start analysis: ${error.message || error}`);
        }
    }

    resetNowcastLoadingScreen() {
        // Reset all steps to pending
        for (let i = 1; i <= 5; i++) {
            const stepElement = document.getElementById(`nowcast-step${i}`);
            if (stepElement) {
                stepElement.className = 'step pending';
            }
        }

        // Reset progress
        const progressFill = document.getElementById('nowcastProgressFill');
        const progressText = document.getElementById('nowcastProgressText');
        if (progressFill) progressFill.style.width = '0%';
        if (progressText) progressText.textContent = '0%';

        // Reset message
        const loadingMessageText = document.getElementById('nowcastLoadingMessageText');
        if (loadingMessageText) {
            loadingMessageText.textContent = 'Please wait while we process your inputs...';
        }

        // Hide error banner
        this.hideNowcastError();

        // Reset cancel button
        const cancelBtn = document.getElementById('cancelNowcastBtn');
        if (cancelBtn) {
            cancelBtn.disabled = false;
            cancelBtn.textContent = 'Cancel';
        }
    }

    setupNowcastCancelButton() {
        const cancelBtn = document.getElementById('cancelNowcastBtn');
        if (cancelBtn) {
            // Remove existing listeners by cloning
            const newCancelBtn = cancelBtn.cloneNode(true);
            cancelBtn.parentNode.replaceChild(newCancelBtn, cancelBtn);

            newCancelBtn.addEventListener('click', async () => {
                await this.cancelNowcastAnalysis();
            });
        }
    }

    async cancelNowcastAnalysis() {
        console.log('Cancelling nowcast analysis...');
        const cancelBtn = document.getElementById('cancelNowcastBtn');
        if (cancelBtn) {
            cancelBtn.disabled = true;
            cancelBtn.textContent = 'Cancelling...';
        }

        try {
            if (this.api && this.api.cancel_nowcast_analysis) {
                const result = await this.api.cancel_nowcast_analysis();
                console.log('Cancel result:', result);
            }
        } catch (error) {
            console.error('Error cancelling nowcast analysis:', error);
        }

        // Stop polling
        if (this.nowcastStatusPollInterval) {
            clearInterval(this.nowcastStatusPollInterval);
            this.nowcastStatusPollInterval = null;
        }

        // Return to nowcast configuration screen
        setTimeout(() => {
            this.showScreen('nowcast-config');
        }, 500);
    }

    startNowcastStatusPolling() {
        // Clear any existing polling
        if (this.nowcastStatusPollInterval) {
            clearInterval(this.nowcastStatusPollInterval);
        }

        // Poll every 2 seconds
        this.nowcastStatusPollInterval = setInterval(async () => {
            try {
                if (!this.api || !this.api.get_nowcast_analysis_status) {
                    return;
                }

                const status = await this.api.get_nowcast_analysis_status();
                this.updateNowcastLoadingScreen(status);

                // Check if completed or error
                if (status.status === 'completed') {
                    clearInterval(this.nowcastStatusPollInterval);
                    this.nowcastStatusPollInterval = null;
                    await this.handleNowcastAnalysisComplete();
                } else if (status.status === 'error') {
                    clearInterval(this.nowcastStatusPollInterval);
                    this.nowcastStatusPollInterval = null;
                    this.showNowcastError(status.error_message || 'Analysis failed');
                } else if (status.status === 'cancelled') {
                    clearInterval(this.nowcastStatusPollInterval);
                    this.nowcastStatusPollInterval = null;
                    // Already handled by cancel button
                }
            } catch (error) {
                console.error('Error polling nowcast status:', error);
            }
        }, 2000); // Poll every 2 seconds
    }

    updateNowcastLoadingScreen(status) {
        console.log('Updating nowcast loading screen:', status);

        // Update progress bar
        const progressFill = document.getElementById('nowcastProgressFill');
        const progressText = document.getElementById('nowcastProgressText');
        if (progressFill && status.progress_percent !== undefined) {
            progressFill.style.width = `${status.progress_percent}%`;
        }
        if (progressText && status.progress_percent !== undefined) {
            progressText.textContent = `${status.progress_percent}%`;
        }

        // Update message
        const loadingMessageText = document.getElementById('nowcastLoadingMessageText');
        if (loadingMessageText && status.message) {
            loadingMessageText.textContent = status.message;
        }

        // Update steps based on current phase
        if (status.current_phase) {
            for (let i = 1; i <= 5; i++) {
                const stepElement = document.getElementById(`nowcast-step${i}`);
                if (stepElement) {
                    if (i < status.current_phase) {
                        stepElement.className = 'step completed';
                    } else if (i === status.current_phase) {
                        stepElement.className = 'step active';
                    } else {
                        stepElement.className = 'step pending';
                    }
                }
            }
        }
    }

    async handleNowcastAnalysisComplete() {
        console.log('Nowcast analysis completed');

        // Update loading screen to show completion
        const progressFill = document.getElementById('nowcastProgressFill');
        const progressText = document.getElementById('nowcastProgressText');
        if (progressFill) progressFill.style.width = '100%';
        if (progressText) progressText.textContent = '100%';

        const loadingMessageText = document.getElementById('nowcastLoadingMessageText');
        if (loadingMessageText) {
            loadingMessageText.textContent = 'Analysis completed! Loading dashboard...';
        }

        // Wait a moment then navigate to dashboard
        setTimeout(() => {
            // Use the select_mode API to navigate
            if (this.api && this.api.select_mode) {
                this.api.select_mode('nowcast')
                    .then(() => console.log('Navigated to nowcast dashboard'))
                    .catch(error => console.error('Error navigating:', error));
            } else {
                // Fallback: show nowcast dashboard screen
                this.currentMode = 'nowcast';
                this.showScreen('nowcast-dashboard');
            }
        }, 1500);
    }

    showNowcastError(message) {
        console.error('Nowcast error:', message);

        const errorBanner = document.getElementById('nowcastErrorBanner');
        const errorMessage = document.getElementById('nowcastErrorMessage');

        if (errorBanner && errorMessage) {
            errorMessage.textContent = message;
            errorBanner.style.display = 'flex';
        }

        // Also update loading message
        const loadingMessageText = document.getElementById('nowcastLoadingMessageText');
        if (loadingMessageText) {
            loadingMessageText.textContent = 'Error: ' + message;
        }

        // Setup close button
        const closeBtn = document.getElementById('closeNowcastErrorBanner');
        if (closeBtn) {
            closeBtn.onclick = () => {
                this.hideNowcastError();
                this.showScreen('nowcast-config');
            };
        }
    }

    hideNowcastError() {
        const errorBanner = document.getElementById('nowcastErrorBanner');
        if (errorBanner) {
            errorBanner.style.display = 'none';
        }
    }

    async runHistoricalAnalysis() {
        console.log('Running historical analysis...');

        // Get configuration
        const country = document.getElementById('historicalCountrySelect').value;
        const year = parseInt(document.getElementById('yearSelect').value);

        console.log(`Configuration: Country=${country}, Year=${year}`);

        // Validate selection
        if (!year || isNaN(year)) {
            alert('Please select a year before running the analysis.');
            return;
        }

        // Check if API is available
        if (!this.api || !this.api.run_historical_analysis) {
            this.showError('API not available. Please ensure the application is running correctly.');
            return;
        }

        // Reset loading screen
        this.resetLoadingScreen();

        // Show loading screen
        this.showScreen('historical-loading');

        // Setup cancel button handler
        this.setupCancelButton();

        try {
            // Start the analysis
            const result = await this.api.run_historical_analysis(country, year, false);
            console.log('Analysis started:', result);

            if (result && result.status === 'error') {
                this.showError(result.message || 'Failed to start analysis');
                return;
            }

            // Start polling for status updates
            this.startStatusPolling();

        } catch (error) {
            console.error('Error starting historical analysis:', error);
            this.showError(`Failed to start analysis: ${error.message || error}`);
        }
    }

    resetLoadingScreen() {
        // Reset all steps to pending
        for (let i = 1; i <= 5; i++) {
            const stepElement = document.getElementById(`loading-step${i}`);
            if (stepElement) {
                stepElement.className = 'step pending';
            }
        }

        // Reset progress
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        if (progressFill) progressFill.style.width = '0%';
        if (progressText) progressText.textContent = '0%';

        // Reset message
        const loadingMessageText = document.getElementById('loadingMessageText');
        if (loadingMessageText) {
            loadingMessageText.textContent = 'Please wait while we process your inputs...';
        }

        // Hide error banner
        this.hideError();

        // Reset cancel button
        const cancelBtn = document.getElementById('cancelAnalysisBtn');
        if (cancelBtn) {
            cancelBtn.disabled = false;
            cancelBtn.textContent = 'Cancel';
        }
    }

    setupCancelButton() {
        const cancelBtn = document.getElementById('cancelAnalysisBtn');
        if (cancelBtn) {
            // Remove existing listeners by cloning
            const newCancelBtn = cancelBtn.cloneNode(true);
            cancelBtn.parentNode.replaceChild(newCancelBtn, cancelBtn);

            newCancelBtn.addEventListener('click', async () => {
                await this.cancelAnalysis();
            });
        }
    }

    async cancelAnalysis() {
        console.log('Cancelling analysis...');
        const cancelBtn = document.getElementById('cancelAnalysisBtn');
        if (cancelBtn) {
            cancelBtn.disabled = true;
            cancelBtn.textContent = 'Cancelling...';
        }

        try {
            if (this.api && this.api.cancel_historical_analysis) {
                const result = await this.api.cancel_historical_analysis();
                console.log('Cancel result:', result);
            }
        } catch (error) {
            console.error('Error cancelling analysis:', error);
        }

        // Stop polling
        if (this.statusPollInterval) {
            clearInterval(this.statusPollInterval);
            this.statusPollInterval = null;
        }

        // Return to historical configuration screen
        setTimeout(() => {
            this.showScreen('historical-config');
        }, 500);
    }

    startStatusPolling() {
        // Clear any existing polling
        if (this.statusPollInterval) {
            clearInterval(this.statusPollInterval);
        }

        // Poll every 500ms
        this.statusPollInterval = setInterval(async () => {
            try {
                if (!this.api || !this.api.get_historical_analysis_status) {
                    return;
                }

                const status = await this.api.get_historical_analysis_status();
                this.updateLoadingScreen(status);

                // Check if completed or error
                if (status.status === 'completed') {
                    clearInterval(this.statusPollInterval);
                    this.statusPollInterval = null;
                    await this.handleAnalysisComplete();
                } else if (status.status === 'error') {
                    clearInterval(this.statusPollInterval);
                    this.statusPollInterval = null;
                    this.showError(status.error_message || 'Analysis failed');
                } else if (status.status === 'cancelled') {
                    clearInterval(this.statusPollInterval);
                    this.statusPollInterval = null;
                    // Already handled by cancel button
                }
            } catch (error) {
                console.error('Error polling status:', error);
            }
        }, 500);
    }

    updateLoadingScreen(status) {
        // Update progress bar
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        const progressPercent = status.progress_percent || 0;

        if (progressFill) {
            progressFill.style.width = `${progressPercent}%`;
        }
        if (progressText) {
            progressText.textContent = `${Math.round(progressPercent)}%`;
        }

        // Update message
        const loadingMessageText = document.getElementById('loadingMessageText');
        if (loadingMessageText && status.message) {
            loadingMessageText.textContent = status.message;
        }

        // Update current phase
        const currentPhase = status.current_phase || 0;
        const totalPhases = status.total_phases || 5;

        // Mark completed phases
        for (let i = 1; i < currentPhase; i++) {
            const stepElement = document.getElementById(`loading-step${i}`);
            if (stepElement) {
                stepElement.className = 'step completed';
            }
        }

        // Mark current phase as in-progress
        if (currentPhase > 0 && currentPhase <= totalPhases) {
            const stepElement = document.getElementById(`loading-step${currentPhase}`);
            if (stepElement) {
                stepElement.className = 'step in-progress';
            }
        }

        // Mark pending phases
        for (let i = currentPhase + 1; i <= totalPhases; i++) {
            const stepElement = document.getElementById(`loading-step${i}`);
            if (stepElement) {
                stepElement.className = 'step pending';
            }
        }
    }

    async handleAnalysisComplete() {
        console.log('Analysis completed successfully');

        // Update loading screen to show completion
        const loadingMessage = document.getElementById('loadingMessage');
        if (loadingMessage) {
            loadingMessage.textContent = 'Analysis completed successfully! Loading dashboard...';
        }

        // Mark all steps as completed
        for (let i = 1; i <= 5; i++) {
            const stepElement = document.getElementById(`loading-step${i}`);
            if (stepElement) {
                stepElement.className = 'step completed';
            }
        }

        // Set progress to 100%
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        if (progressFill) progressFill.style.width = '100%';
        if (progressText) progressText.textContent = '100%';

        // Disable cancel button
        const cancelBtn = document.getElementById('cancelAnalysisBtn');
        if (cancelBtn) {
            cancelBtn.disabled = true;
        }

        // Wait a moment for user to see completion, then load dashboard
        setTimeout(async () => {
            await this.loadHistoricalDashboard();
        }, 1500);
    }

    showError(message) {
        const errorBanner = document.getElementById('errorBanner');
        const errorMessage = document.getElementById('errorMessage');
        if (errorBanner && errorMessage) {
            errorMessage.textContent = message;
            errorBanner.style.display = 'flex';
        }
    }

    hideError() {
        const errorBanner = document.getElementById('errorBanner');
        if (errorBanner) {
            errorBanner.style.display = 'none';
        }
    }

    showLoadingState(mode) {
        const content = document.getElementById(`${mode}-content`);
        if (content) {
            content.innerHTML = `
                <div class="loading">
                    <h3>Running ${mode.charAt(0).toUpperCase() + mode.slice(1)} Analysis...</h3>
                    <p>Please wait while we process your inputs...</p>
                    <div class="loading-steps">
                        <div class="step" id="step1">⏳ Downloaded VIRS boat detections</div>
                        <div class="step" id="step2">⏳ Recreated cyclone tracks</div>
                        <div class="step" id="step3">⏳ Generated impact dashboards</div>
                        <div class="step" id="step4">⏳ Running results dashboard</div>
                    </div>
                </div>
            `;
        }
    }

    startLoadingAnimation() {
        const steps = [
            'Downloaded VIRS boat detections',
            'Recreated cyclone tracks',
            'Generated impact dashboards',
            'Running results dashboard'
        ];

        let currentStep = 0;
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');

        const animateStep = () => {
            if (currentStep < steps.length) {
                // Update current step
                const stepElement = document.getElementById(`loading-step${currentStep + 1}`);
                if (stepElement) {
                    stepElement.className = 'step in-progress';
                }

                // Update progress bar
                const progress = ((currentStep + 1) / steps.length) * 100;
                if (progressFill) {
                    progressFill.style.width = `${progress}%`;
                }
                if (progressText) {
                    progressText.textContent = `${Math.round(progress)}%`;
                }

                // Mark previous step as completed
                if (currentStep > 0) {
                    const prevStepElement = document.getElementById(`loading-step${currentStep}`);
                    if (prevStepElement) {
                        prevStepElement.className = 'step completed';
                        prevStepElement.innerHTML = `✓ ${steps[currentStep - 1]}`;
                    }
                }

                currentStep++;

                // Continue animation
                setTimeout(animateStep, 500);
            } else {
                // Mark last step as completed
                const lastStepElement = document.getElementById(`loading-step${steps.length}`);
                if (lastStepElement) {
                    lastStepElement.className = 'step completed';
                    lastStepElement.innerHTML = `✓ ${steps[steps.length - 1]}`;
                }
            }
        };

        // Start animation
        animateStep();
    }

    startNowcastLoadingAnimation() {
        console.log('Starting nowcast loading animation...');
        const steps = [
            'Downloaded VIRS boat detections',
            'Recreated cyclone tracks',
            'Generated impact dashboards',
            'Running results dashboard'
        ];

        let currentStep = 0;
        const progressFill = document.getElementById('nowcastProgressFill');
        const progressText = document.getElementById('nowcastProgressText');

        console.log('Progress elements found:', { progressFill, progressText });

        const animateStep = () => {
            console.log(`Animating step ${currentStep + 1}`);
            if (currentStep < steps.length) {
                // Update current step
                const stepElement = document.getElementById(`nowcast-loading-step${currentStep + 1}`);
                console.log(`Step element ${currentStep + 1}:`, stepElement);
                if (stepElement) {
                    stepElement.className = 'step in-progress';
                }

                // Update progress bar
                const progress = ((currentStep + 1) / steps.length) * 100;
                if (progressFill) {
                    progressFill.style.width = `${progress}%`;
                }
                if (progressText) {
                    progressText.textContent = `${Math.round(progress)}%`;
                }

                // Mark previous step as completed
                if (currentStep > 0) {
                    const prevStepElement = document.getElementById(`nowcast-loading-step${currentStep}`);
                    if (prevStepElement) {
                        prevStepElement.className = 'step completed';
                        prevStepElement.innerHTML = `✓ ${steps[currentStep - 1]}`;
                    }
                }

                currentStep++;

                // Continue animation
                setTimeout(animateStep, 500);
            } else {
                // Mark last step as completed
                const lastStepElement = document.getElementById(`nowcast-loading-step${steps.length}`);
                if (lastStepElement) {
                    lastStepElement.className = 'step completed';
                    lastStepElement.innerHTML = `✓ ${steps[steps.length - 1]}`;
                }
                console.log('Loading animation completed');
            }
        };

        // Start animation
        animateStep();
    }

    updateLoadingSteps(mode, completedStep) {
        const steps = [
            'Downloaded VIRS boat detections',
            'Recreated cyclone tracks',
            'Generated impact dashboards',
            'Running results dashboard'
        ];

        // Update completed steps
        for (let i = 1; i <= completedStep; i++) {
            const stepElement = document.getElementById(`step${i}`);
            if (stepElement) {
                stepElement.className = 'step completed';
                stepElement.innerHTML = `✓ ${steps[i-1]}`;
            }
        }

        // Update current step
        if (completedStep < steps.length) {
            const currentStepElement = document.getElementById(`step${completedStep + 1}`);
            if (currentStepElement) {
                currentStepElement.className = 'step in-progress';
                currentStepElement.innerHTML = `⏳ ${steps[completedStep]}`;
            }
        }
    }

    async loadNowcastDashboard() {
        console.log('Loading nowcast dashboard...');

        // Use the API to load the nowcast dashboard
        // Python will handle navigation via evaluate_js
        if (this.api && this.api.load_nowcast_dashboard) {
            await this.api.load_nowcast_dashboard();
        } else {
            // Fallback: navigate to the nowcast dashboard
            window.location.href = 'nowcast/index.html';
        }
    }


    async loadHistoricalDashboard() {
        console.log('Loading historical dashboard...');

        // Wait for API to be available
        if (!this.api) {
            console.log('API not ready yet, waiting...');
            // Wait a bit and try again
            setTimeout(() => {
                this.loadHistoricalDashboard();
            }, 500);
            return;
        }

        try {
            console.log('API is ready, loading historical POC UI...');

            // Use the API to load the historical dashboard
            // Python will handle navigation via evaluate_js
            await this.api.load_historical_dashboard();

        } catch (error) {
            console.error('Error loading historical dashboard:', error);
            // Fallback
            window.location.href = 'historical/index.html';
        }
    }

    initializeHistoricalDashboard(data) {
        console.log('Initializing historical dashboard with data:', data);

        // This is a simplified version - in a real implementation,
        // you would initialize all the charts, tables, and maps here
        const content = document.getElementById('historical-content');
        if (content) {
            const typhoonCount = Object.keys(data.typhoons || {}).length;
            const fishingGroundCount = data.fishing_grounds ? data.fishing_grounds.length : 0;

            content.innerHTML = `
                <div class="dashboard-content">
                    <h3>Historical Dashboard Ready</h3>
                    <p>Data loaded successfully from the database.</p>
                    <div class="stats">
                        <div class="stat">
                            <span class="stat-label">Available Typhoons:</span>
                            <span class="stat-value">${typhoonCount}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Fishing Grounds:</span>
                            <span class="stat-value">${fishingGroundCount}</span>
                        </div>
                    </div>
                    <div class="typhoon-list">
                        <h4>Available Typhoons:</h4>
                        <ul>
                            ${Object.keys(data.typhoons || {}).map(name =>
                                `<li>${name} (${data.typhoons[name].year || 'N/A'})</li>`
                            ).join('')}
                        </ul>
                    </div>
                </div>
            `;
        }
    }

    // Additional methods for dashboard interactions
    exploreTyphoons() {
        alert('Typhoon exploration feature - would show detailed typhoon list and selection');
    }

    viewImpactAnalysis() {
        alert('Impact analysis feature - would show charts and visualizations');
    }

    exportData() {
        alert('Data export feature - would allow downloading analysis results');
    }

    /**
     * Read a file as base64 string
     * @param {File} file - File to read
     * @returns {Promise<string>} Base64 encoded file data
     */
    readFileAsBase64(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => {
                // Remove data URL prefix (e.g., "data:application/octet-stream;base64,")
                const base64 = reader.result.split(',')[1] || reader.result;
                resolve(base64);
            };
            reader.onerror = reject;
            reader.readAsDataURL(file);
        });
    }

}

/**
 * Track Drawer Class
 * Handles typhoon track drawing functionality
 */
class TrackDrawer {
    constructor() {
        this.map = null;
        this.trackPoints = [];
        this.isPointMode = false;
        this.currentClickCoords = null;
        this.trackLine = null;
        this.typhoonIcon = null;
        this.isOpen = false;
        this.hasUnsavedChanges = false;
        this.savedTrackPath = null;
        this.api = null;

        this.init();
    }

    init() {
        // Wait for API to be available
        this.waitForAPI();
    }

    waitForAPI() {
        const checkAPI = () => {
            if (window.pywebview && window.pywebview.api) {
                this.api = window.pywebview.api;
                console.log('TrackDrawer API initialized');
                return true;
            }
            return false;
        };

        if (!checkAPI()) {
            setTimeout(() => {
                if (!checkAPI()) {
                    console.warn('TrackDrawer: PyWebView API not available, running in browser mode');
                }
            }, 100);
        }
    }

    open() {
        console.log('Opening track drawer...');
        const modal = document.getElementById('trackDrawerModal');
        if (!modal) {
            console.error('Track drawer modal not found!');
            return;
        }

        modal.classList.add('show');
        this.isOpen = true;
        this.hasUnsavedChanges = false;

        // Initialize map if not already initialized
        if (!this.map) {
            this.initializeMap();
        } else {
            // Invalidate size to ensure map displays correctly
            setTimeout(() => {
                this.map.invalidateSize();
            }, 100);
        }

        // Hide loading overlay after a short delay
        setTimeout(() => {
            const loadingOverlay = document.getElementById('trackDrawerLoadingOverlay');
            if (loadingOverlay) {
                loadingOverlay.style.display = 'none';
            }
        }, 500);

        // Setup event listeners
        this.setupEventListeners();
    }

    close() {
        if (this.hasUnsavedChanges && this.trackPoints.length > 0) {
            const shouldSave = confirm('You have unsaved changes. Do you want to save the current track before closing?');
            if (shouldSave) {
                this.downloadTrack().then(() => {
                    this.closeModal();
                }).catch(() => {
                    // User cancelled or error occurred
                });
                return;
            }
        }
        this.closeModal();
    }

    closeModal() {
        const modal = document.getElementById('trackDrawerModal');
        if (modal) {
            modal.classList.remove('show');
        }
        this.isOpen = false;
        this.isPointMode = false;

        // Reset point mode button
        const pointButton = document.getElementById('trackDrawerPointButton');
        if (pointButton) {
            pointButton.textContent = 'Add Points';
            pointButton.style.background = '#2563eb';
        }

        // Reopen synthetic modal with Draw tab active
        this.reopenSyntheticModalWithDrawTab();
    }

    reopenSyntheticModalWithDrawTab() {
        const syntheticModal = document.getElementById('syntheticModal');
        const syntheticTabs = document.querySelectorAll('.synthetic-source-btn');
        const drawTab = document.getElementById('drawTab');

        if (!syntheticModal) {
            return;
        }

        // Set Draw tab as active
        syntheticTabs.forEach(tab => {
            const tabName = tab.getAttribute('data-tab');
            if (tabName === 'draw') {
                tab.classList.add('active');
            } else {
                tab.classList.remove('active');
            }
        });

        // Hide all tab panels
        document.querySelectorAll('.tab-panel').forEach(panel => {
            panel.style.display = 'none';
        });

        // Show Draw tab panel
        if (drawTab) {
            drawTab.style.display = 'block';
        }

        // If there's a saved drawn track, also update the Upload tab to show it
        const savedTrackPath = window.savedDrawnTrackPath || (window.trackDrawer ? window.trackDrawer.getSavedTrackPath() : null);
        if (savedTrackPath && window.trackDrawer) {
            window.trackDrawer.setSavedTrackAsUploaded(savedTrackPath);
        }

        // Open synthetic modal
        syntheticModal.classList.add('show');
    }

    initializeMap() {
        console.log('Initializing map...');
        const mapContainer = document.getElementById('trackDrawerMap');
        if (!mapContainer) {
            console.error('Map container not found!');
            return;
        }

        // Center map on Philippines with appropriate zoom level
        this.map = L.map('trackDrawerMap').setView([12.8797, 121.7740], 5);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(this.map);

        // Custom marker icon for typhoon points
        this.typhoonIcon = L.divIcon({
            className: 'typhoon-marker',
            html: '<div style="background: #2563eb; border: 2px solid white; border-radius: 50%; width: 12px; height: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>',
            iconSize: [12, 12],
            iconAnchor: [6, 6]
        });

        // Map click handler
        this.map.on('click', (e) => {
            if (this.isPointMode) {
                this.showPointModal([e.latlng.lng, e.latlng.lat]);
            }
        });

        // Handle window resize to update map size
        window.addEventListener('resize', () => {
            if (this.map) {
                this.map.invalidateSize();
            }
        });
    }

    setupEventListeners() {
        // Close button
        const closeBtn = document.getElementById('trackDrawerCloseBtn');
        if (closeBtn) {
            closeBtn.onclick = () => this.close();
        }

        // Point form submission
        const pointForm = document.getElementById('trackDrawerPointForm');
        if (pointForm) {
            pointForm.onsubmit = (e) => {
                e.preventDefault();
                this.handleFormSubmit();
            };
        }

        // Auto-close date picker when date is selected
        const dateInput = document.getElementById('trackDrawerDateInput');
        if (dateInput) {
            dateInput.addEventListener('change', (e) => {
                e.target.blur();
            });
        }

        // Close point modal when clicking outside
        const pointModal = document.getElementById('trackDrawerPointModal');
        if (pointModal) {
            pointModal.addEventListener('click', (e) => {
                if (e.target === pointModal) {
                    this.cancelPoint();
                }
            });
        }
    }

    togglePointMode() {
        if (!this.map) {
            console.error('Map not initialized!');
            return;
        }

        const pointButton = document.getElementById('trackDrawerPointButton');
        if (!pointButton) {
            console.error('Point button not found!');
            return;
        }

        this.isPointMode = !this.isPointMode;

        if (this.isPointMode) {
            pointButton.textContent = 'Stop Adding Points';
            pointButton.style.background = '#dc3545';
            this.map.getContainer().style.cursor = 'crosshair';
        } else {
            pointButton.textContent = 'Add Points';
            pointButton.style.background = '#2563eb';
            this.map.getContainer().style.cursor = '';
        }
    }

    clearTrack() {
        if (this.trackPoints.length === 0) {
            return;
        }

        if (!confirm('Are you sure you want to clear all track points?')) {
            return;
        }

        this.trackPoints = [];
        if (this.map) {
            this.map.eachLayer((layer) => {
                if (layer instanceof L.Marker || layer instanceof L.Polyline) {
                    this.map.removeLayer(layer);
                }
            });
        }
        this.trackLine = null;
        this.updateTrackTable();
        this.updatePointCount();
        this.hasUnsavedChanges = true;
    }

    showPointModal(coords) {
        this.currentClickCoords = coords;
        const pointModal = document.getElementById('trackDrawerPointModal');
        if (pointModal) {
            pointModal.classList.add('show');
        }

        // Set default date to current date
        const now = new Date();
        const localDate = now.toISOString().split('T')[0];
        const dateInput = document.getElementById('trackDrawerDateInput');
        if (dateInput) {
            dateInput.value = localDate;
        }

        // Set default hour to nearest ibtracs time
        const currentHour = now.getHours();
        const ibtracsHours = [0, 3, 6, 9, 12, 15, 18, 21];
        const nearestHour = ibtracsHours.reduce((prev, curr) =>
            Math.abs(curr - currentHour) < Math.abs(prev - currentHour) ? curr : prev
        );
        const hourInput = document.getElementById('trackDrawerHourInput');
        if (hourInput) {
            hourInput.value = nearestHour.toString().padStart(2, '0');
        }

        // Focus on first input
        if (dateInput) {
            dateInput.focus();
        }
    }

    cancelPoint() {
        const pointModal = document.getElementById('trackDrawerPointModal');
        if (pointModal) {
            pointModal.classList.remove('show');
        }
        const pointForm = document.getElementById('trackDrawerPointForm');
        if (pointForm) {
            pointForm.reset();
        }
        this.currentClickCoords = null;
    }

    handleFormSubmit() {
        const dateInput = document.getElementById('trackDrawerDateInput');
        const hourInput = document.getElementById('trackDrawerHourInput');
        const cycloneSpeedInput = document.getElementById('trackDrawerCycloneSpeed');
        const windSpeedInput = document.getElementById('trackDrawerWindSpeed');

        if (!dateInput || !hourInput || !cycloneSpeedInput || !windSpeedInput) {
            console.error('Form inputs not found!');
            return;
        }

        const pointData = {
            coordinates: this.currentClickCoords,
            date_time: dateInput.value + 'T' + hourInput.value + ':00',
            cyclone_spd: parseFloat(cycloneSpeedInput.value),
            wind_spd: parseFloat(windSpeedInput.value)
        };

        this.addPointToTrack(pointData);
        this.cancelPoint();
    }

    addPointToTrack(pointData) {
        if (!this.map) {
            console.error('Map not initialized!');
            return;
        }

        this.trackPoints.push(pointData);
        this.hasUnsavedChanges = true;

        // Add marker to map
        const marker = L.marker([pointData.coordinates[1], pointData.coordinates[0]], {
            icon: this.typhoonIcon
        }).addTo(this.map);

        // Add popup with point info
        marker.bindPopup(`
            <div style="font-size: 12px;">
                <strong>Date:</strong> ${new Date(pointData.date_time).toLocaleString()}<br>
                <strong>Cyclone Speed (kts):</strong> ${pointData.cyclone_spd} kts<br>
                <strong>Wind Speed (kts):</strong> ${pointData.wind_spd} kts
            </div>
        `);

        // Update track line
        this.updateTrackLine();
        this.updateTrackTable();
        this.updatePointCount();
    }

    updateTrackLine() {
        if (!this.map) {
            return;
        }

        if (this.trackLine) {
            this.map.removeLayer(this.trackLine);
        }

        if (this.trackPoints.length > 1) {
            const coords = this.trackPoints.map(p => [p.coordinates[1], p.coordinates[0]]);
            this.trackLine = L.polyline(coords, {
                color: '#2563eb',
                weight: 3,
                opacity: 0.7
            }).addTo(this.map);
        }
    }

    updateTrackTable() {
        const container = document.getElementById('trackDrawerTableContainer');
        if (!container) {
            return;
        }

        if (this.trackPoints.length === 0) {
            container.innerHTML = '<div class="track-drawer-no-data">No track points added yet.</div>';
            return;
        }

        let tableHTML = `
            <table class="track-drawer-table-compact">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Date & Time</th>
                        <th>Lat</th>
                        <th>Lng</th>
                        <th>Cyclone Speed (kts)</th>
                        <th>Wind Speed (kts)</th>
                    </tr>
                </thead>
                <tbody>
        `;

        this.trackPoints.forEach((point, index) => {
            const dateTime = new Date(point.date_time);
            const formattedDate = dateTime.toLocaleDateString('en-CA').slice(5); // MM-DD
            const formattedTime = dateTime.toLocaleTimeString('en-GB', {hour: '2-digit', minute: '2-digit'}); // HH:MM
            const lat = point.coordinates[1].toFixed(2);
            const lng = point.coordinates[0].toFixed(2);

            tableHTML += `
                <tr>
                    <td>${index + 1}</td>
                    <td title="${dateTime.toISOString().replace('T', ' ').slice(0, 16)}">${formattedDate}<br/>${formattedTime}</td>
                    <td>${lat}</td>
                    <td>${lng}</td>
                    <td>${point.cyclone_spd}</td>
                    <td>${point.wind_spd}</td>
                </tr>
            `;
        });

        tableHTML += '</tbody></table>';
        container.innerHTML = tableHTML;
    }

    updatePointCount() {
        const pointCount = document.getElementById('trackDrawerPointCount');
        if (pointCount) {
            pointCount.textContent = this.trackPoints.length;
        }
    }

    async downloadTrack() {
        if (this.trackPoints.length === 0) {
            alert('Please add some track points first!');
            return Promise.reject('No points to save');
        }

        try {
            const trackData = {
                points: this.trackPoints
            };

            const dataStr = JSON.stringify(trackData);

            if (!this.api || !this.api.save_track) {
                console.error('API not available or save_track method not found');
                alert('Unable to save track. API not available.');
                return Promise.reject('API not available');
            }

            const result = await this.api.save_track(dataStr);
            if (result) {
                this.savedTrackPath = result;
                this.hasUnsavedChanges = false;
                alert('Track saved successfully!');

                // Update draw tab status
                const drawTabStatus = document.getElementById('drawTabStatus');
                if (drawTabStatus) {
                    drawTabStatus.innerHTML = `<p style="color: #10b981;">Track saved: ${result}</p>`;
                }

                // Update Upload tab to show the saved track as uploaded file
                this.setSavedTrackAsUploaded(result);

                return Promise.resolve(result);
            } else {
                alert('Failed to save track. Please try again.');
                return Promise.reject('Save failed');
            }
        } catch (error) {
            console.error('Error saving track:', error);
            alert('Error saving track: ' + error);
            return Promise.reject(error);
        }
    }

    setSavedTrackAsUploaded(trackPath) {
        // Update the file input display to show the saved track
        const fileInputText = document.querySelector('.file-input-text');
        if (fileInputText) {
            // Extract filename from path
            const filename = trackPath.split(/[/\\]/).pop();
            fileInputText.textContent = filename;
            fileInputText.style.color = '#10b981'; // Green color to indicate saved
        }

        // Store the saved track path globally so it can be accessed by Proceed button
        window.savedDrawnTrackPath = trackPath;

        // Also store in the app instance if available
        if (window.app) {
            window.app.savedDrawnTrackPath = trackPath;
        }
    }

    getSavedTrackPath() {
        return this.savedTrackPath || window.savedDrawnTrackPath || null;
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new CycloneToolkitApp();
    window.trackDrawer = new TrackDrawer();
});
