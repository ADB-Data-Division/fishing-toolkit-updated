// Dashboard Application
class TyphoonDashboard {
    constructor() {
        this.map = null;
        this.chart = null;
        this.currentTyphoon = null;
        this.currentDate = null;
        this.typhoonData = {};
        this.fishingGrounds = [];
        this.typhoonLayers = {};
        this.typhoonList = [];

        this.init();
    }

    async init() {
        try {
            console.log('Starting dashboard initialization...');
            // Bridge console logs to Python if available
            if (window.pywebview && window.pywebview.api && window.pywebview.api.console_log) {
                window.pywebview.api.console_log('info', 'Starting dashboard initialization...');
            }
            await this.loadDataFromAPI();
            this.setupEventListeners();
            await this.initMap();

            // Only initialize chart and update display if we have data
            if (this.currentTyphoon && this.typhoonData[this.currentTyphoon]) {
                this.initChart();
                this.updateDisplay();
            } else {
                console.warn('No typhoon data available for display');
            }

            console.log('Dashboard initialization complete');
        } catch (error) {
            if (window.pywebview && window.pywebview.api && window.pywebview.api.console_log) {
                window.pywebview.api.console_log('error', 'Error initializing dashboard: ' + error.toString());
            }
            console.error('Error initializing dashboard:', error);
        }
    }

    async loadDataFromAPI() {
        // Try multiple times to load data from API
        for (let attempt = 1; attempt <= 5; attempt++) {
            try {
                console.log(`Loading data from PyWebView API (attempt ${attempt})...`);

                            // Check if PyWebView API is available
            if (!window.pywebview || !window.pywebview.api) {
                console.log(`PyWebView API not available yet (attempt ${attempt}), waiting...`);
                await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
                continue;
            }

            // Load dashboard data from the exposed Python API
            const dashboardData = await window.pywebview.api.get_dashboard_data();
                console.log('Dashboard data received:', dashboardData);

                if (dashboardData && dashboardData.typhoons && dashboardData.typhoons.length > 0) {
                    this.typhoonList = dashboardData.typhoons;
                    this.fishingGrounds = dashboardData.fishing_grounds;

                    // Set default typhoon and date
                    this.currentTyphoon = dashboardData.typhoons[0].uuid;
                    this.currentDate = dashboardData.default_dates[0] || null;

                    console.log('Set currentTyphoon to:', this.currentTyphoon);
                    console.log('Set currentDate to:', this.currentDate);

                    // Load the current typhoon's data
                    await this.loadTyphoonData(this.currentTyphoon);

                    // Update selectors
                    this.updateTyphoonSelector();
                    await this.updateDateSelector();

                    console.log(`Loaded ${this.typhoonList.length} typhoons`);
                    console.log(`Current typhoon: ${this.currentTyphoon}`);
                    console.log(`Current date: ${this.currentDate}`);
                    return; // Success, exit the retry loop
                } else {
                    console.warn('No typhoon data available from API');
                    this.loadDefaultData();
                    return;
                }

            } catch (error) {
                console.error(`Error loading data from API (attempt ${attempt}):`, error);
                if (attempt === 5) {
                    console.error('All attempts failed, loading default data');
                    this.loadDefaultData();
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second before retry
            }
        }
    }

    async loadTyphoonData(typhoonUuid) {
        try {
            console.log(`Loading data for typhoon: ${typhoonUuid}`);

            // Get typhoon data from API
            const typhoonData = await window.pywebview.api.get_typhoon_data(typhoonUuid);
            console.log('Raw typhoon data:', typhoonData);

            if (typhoonData) {
                // Transform the data to match the dashboard format
                this.typhoonData = this.transformTyphoonData(typhoonData);
                console.log('Transformed typhoon data:', this.typhoonData);
                console.log('currentTyphoon after transform:', this.currentTyphoon);
                console.log('typhoonData keys:', Object.keys(this.typhoonData));
            } else {
                console.error('Failed to load typhoon data');
            }

        } catch (error) {
            console.error('Error loading typhoon data:', error);
        }
    }

    transformTyphoonData(apiData) {
        // Transform API data to dashboard format
        const transformed = {};

        // Create a key for the typhoon (using UUID as key)
        const typhoonKey = apiData.uuid;

        // Get available dates for this typhoon
        const availableDates = Object.keys(apiData.daily_data).sort();
        console.log('Available dates for this typhoon:', availableDates);
        console.log('Current date before check:', this.currentDate);

        // Check if current date is valid for this typhoon, if not use first available
        if (!this.currentDate || !apiData.daily_data[this.currentDate]) {
            this.currentDate = availableDates[0];
            console.log('Updated current date to:', this.currentDate);
        }

        // Get daily data for current date
        const dailyData = apiData.daily_data[this.currentDate];
        console.log('Daily data for', this.currentDate, ':', dailyData);

        if (dailyData) {
            transformed[typhoonKey] = {
                name: apiData.name,
                type: apiData.type,
                dateRange: this.getDateRange(apiData.daily_data),
                date: this.currentDate,
                avgStormSpeed: dailyData.avgStormSpeed,
                maxStormSpeed: dailyData.maxStormSpeed,
                maxWindSpeed: dailyData.maxWindSpeed,
                closestGround: this.getClosestGround(dailyData.distances),
                minDistance: this.getMinDistance(dailyData.distances),
                boatCounts: dailyData.boatCounts,
                distances: dailyData.distances,
                activityDifference: dailyData.activityDifference,
                track: apiData.track_points
            };
        }

        return transformed;
    }

    getDateRange(dailyData) {
        const dates = Object.keys(dailyData).sort();
        if (dates.length === 1) {
            return dates[0];
        }
        return `${dates[0]} to ${dates[dates.length - 1]}`;
    }

    getClosestGround(distances) {
        const minDistance = Math.min(...distances);
        const groundIndex = distances.indexOf(minDistance);
        return `Ground ${groundIndex}`;
    }

    getMinDistance(distances) {
        const minDistance = Math.min(...distances);
        return `${minDistance.toFixed(1)} km`;
    }

    loadDefaultData() {
        console.log('Loading default data...');
        // This should only be used if API completely fails
        this.typhoonData = {};
        this.fishingGrounds = [];
        this.currentTyphoon = null;
        this.currentDate = null;

        console.warn('No data available - dashboard will be empty');
    }

    initChart() {
        const ctx = document.getElementById('boatCountChart');
        if (!ctx) {
            console.error('Chart canvas element not found');
            return;
        }

        console.log('Initializing chart...');
        console.log('Current typhoon for chart:', this.currentTyphoon);
        console.log('Available typhoon data keys:', Object.keys(this.typhoonData || {}));

        const data = this.typhoonData[this.currentTyphoon];

        if (!data) {
            console.error('No data found for typhoon:', this.currentTyphoon);
            console.error('Available data:', this.typhoonData);
            return;
        }

        console.log('Chart data:', data);

        // Destroy existing chart if it exists
        if (this.chart) {
            this.chart.destroy();
        }

        this.chart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Ground 0', 'Ground 1', 'Ground 2', 'Ground 3'],
                datasets: [{
                    label: 'Baseline',
                    data: data.boatCounts.baseline,
                    backgroundColor: '#ffd54f',
                    borderColor: '#ffb300',
                    borderWidth: 1
                }, {
                    label: 'Predicted',
                    data: data.boatCounts.predicted,
                    backgroundColor: '#424242',
                    borderColor: '#212121',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        grid: {
                            color: '#f0f0f0'
                        },
                        ticks: {
                            color: '#666',
                            stepSize: 20
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        },
                        ticks: {
                            color: '#666'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + context.parsed.y + ' boats';
                            }
                        }
                    }
                },
                animation: {
                    duration: 800,
                    easing: 'easeInOutQuart'
                }
            }
        });
    }

    initMap() {
        return new Promise((resolve, reject) => {
            try {
                const mapElement = document.getElementById('map');
                if (!mapElement) {
                    console.error('Map element not found');
                    reject(new Error('Map element not found'));
                    return;
                }

                console.log('Initializing map...');

                // Initialize map centered on Philippines
                this.map = L.map('map', {
                    center: [13.0, 122.0],
                    zoom: 6,
                    zoomControl: true
                });

                // Primary basemap (OpenStreetMap)
                const primaryBasemap = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    attribution: '© OpenStreetMap contributors',
                    maxZoom: 18
                });

                // Backup basemap (CartoDB Positron - faster loading)
                const backupBasemap = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                    attribution: '© CARTO',
                    maxZoom: 20
                });

                // Add primary basemap
                primaryBasemap.addTo(this.map);

                // Set a timeout to switch to backup basemap if primary takes too long
                let basemapTimeout = setTimeout(() => {
                    console.log('Primary basemap taking too long, switching to backup...');
                    this.map.removeLayer(primaryBasemap);
                    backupBasemap.addTo(this.map);

                    // Add a visual indicator that backup basemap is being used
                    const backupNotice = L.control({ position: 'topright' });
                    backupNotice.onAdd = function() {
                        const div = L.DomUtil.create('div', 'backup-basemap-notice');
                        div.innerHTML = '<span style="background: rgba(255,193,7,0.9); padding: 5px 10px; border-radius: 4px; font-size: 12px; color: #000;">⚠️ Backup Basemap</span>';
                        return div;
                    };
                    backupBasemap.on('loading', () => {
                        if (this.map.hasLayer(backupNotice)) {
                            this.map.removeControl(backupNotice);
                        }
                    });
                    backupBasemap.on('load', () => {
                        backupNotice.addTo(this.map);
                    });
                }, 10000); // 10 seconds timeout

                // Clear timeout if primary basemap loads successfully
                primaryBasemap.on('load', () => {
                    console.log('Primary basemap loaded successfully');
                    clearTimeout(basemapTimeout);
                });

                // Wait for map to be ready
                this.map.whenReady(() => {
                    console.log('Map is ready');

                    // Add fishing grounds
                    this.fishingGrounds.forEach(ground => {
                        if (ground.geometry && ground.geometry.type === 'Polygon') {
                            // Add polygon for fishing ground area
                            const polygon = L.polygon(ground.geometry.coordinates[0].map(coord => [coord[1], coord[0]]), {
                                color: '#4caf50',
                                weight: 2,
                                opacity: 0.8,
                                fillColor: '#4caf50',
                                fillOpacity: 0.1
                            }).addTo(this.map);

                            // Add popup with ground info
                            polygon.bindPopup(`<b>${ground.name}</b><br/>${ground.description || ''}`);
                        } else {
                            // Fallback to circle marker if no geometry
                            L.circleMarker([ground.lat, ground.lng], {
                                radius: 8,
                                fillColor: '#4caf50',
                                color: '#2e7d32',
                                weight: 2,
                                opacity: 1,
                                fillOpacity: 0.8
                            }).addTo(this.map).bindPopup(`<b>${ground.name}</b><br/>${ground.description || ''}`);
                        }
                    });

                    this.updateTyphoonTrack();
                    resolve();
                });

            } catch (error) {
                console.error('Error initializing map:', error);
                reject(error);
            }
        });
    }

    updateTyphoonTrack() {
        if (!this.map) return;

        // Clear existing typhoon layers
        Object.values(this.typhoonLayers).forEach(layer => {
            this.map.removeLayer(layer);
        });
        this.typhoonLayers = {};

        const data = this.typhoonData[this.currentTyphoon];
        if (!data) return;

        // Create typhoon track
        const trackCoords = data.track.map(point => [point.lat, point.lng]);

        // Add track line
        const trackLine = L.polyline(trackCoords, {
            color: '#ff5722',
            weight: 3,
            opacity: 0.8
        }).addTo(this.map);

        this.typhoonLayers.trackLine = trackLine;

        // Add clickable points along the track
        data.track.forEach((point, index) => {
            const marker = L.circleMarker([point.lat, point.lng], {
                radius: 6,
                fillColor: '#ff5722',
                color: '#d32f2f',
                weight: 2,
                opacity: 1,
                fillOpacity: 0.9
            }).addTo(this.map);

            // Create popup content
            const popupContent = `
                <div class="typhoon-popup">
                    <h4>${data.name} (${data.type})</h4>
                    <p><span class="popup-label">Date Time:</span> ${point.datetime}</p>
                    <p><span class="popup-label">Wind Speed:</span> ${point.windSpeed} knots</p>
                    <p><span class="popup-label">Cyclone Speed:</span> ${point.cycloneSpeed} knots</p>
                    <p><span class="popup-label">Position:</span> ${point.lat.toFixed(3)}, ${point.lng.toFixed(3)}</p>
                </div>
            `;

            marker.bindPopup(popupContent);
            this.typhoonLayers[`point${index}`] = marker;
        });
    }

    updateChart() {
        if (!this.chart) {
            console.log('Chart not initialized, initializing now...');
            this.initChart();
            return;
        }

        const data = this.typhoonData[this.currentTyphoon];

        if (!data) {
            console.error('No data found for typhoon:', this.currentTyphoon);
            return;
        }

        console.log('Updating chart with data for:', this.currentTyphoon);
        this.chart.data.datasets[0].data = data.boatCounts.baseline;
        this.chart.data.datasets[1].data = data.boatCounts.predicted;

        this.chart.update('active');
    }

    updateDistanceChart() {
        const data = this.typhoonData[this.currentTyphoon];
        if (!data || !data.distances) return;

        const maxDistance = Math.max(...data.distances);
        const distanceChart = document.getElementById('distanceChart');

        if (!distanceChart) return;

        // Clear existing content
        distanceChart.innerHTML = '';

        // Create distance rows dynamically
        data.distances.forEach((distance, index) => {
            const row = document.createElement('div');
            row.className = 'distance-row';

            const percentage = (distance / maxDistance) * 100;

            row.innerHTML = `
                <div class="ground-label">Ground ${index}</div>
                <div class="distance-bar">
                    <div class="distance-fill" data-distance="${distance}" style="width: ${percentage}%"></div>
                </div>
                <div class="distance-value">${distance.toFixed(1)}</div>
            `;

            distanceChart.appendChild(row);
        });

        // Update scale
        const scaleElement = document.querySelector('.distance-scale');
        if (scaleElement && maxDistance > 0) {
            const maxScale = Math.ceil(maxDistance / 400) * 400; // Round up to nearest 400
            scaleElement.innerHTML = `
                <span>0</span>
                <span>${maxScale * 0.33}</span>
                <span>${maxScale * 0.67}</span>
                <span>${maxScale}</span>
            `;
        }
    }

    updateActivityTable() {
        const data = this.typhoonData[this.currentTyphoon];
        const tbody = document.querySelector('#activityTable tbody');

        if (!tbody) return;

        tbody.innerHTML = '';

        data.activityDifference.forEach((diff, index) => {
            const row = document.createElement('tr');
            const isPositive = diff.startsWith('+');

            row.innerHTML = `
                <td>Ground ${index}</td>
                <td class="${isPositive ? 'positive' : 'negative'}">${diff}</td>
            `;

            tbody.appendChild(row);
        });
    }

    updateDisplay() {
        if (!this.currentTyphoon || !this.typhoonData[this.currentTyphoon]) {
            console.warn('No typhoon data available for display');
            return;
        }

        const data = this.typhoonData[this.currentTyphoon];

        // Update metric cards
        const elements = {
            'typhoonName': data.name,
            'dateRange': data.dateRange,
            'avgStormSpeed': data.avgStormSpeed,
            'maxStormSpeed': data.maxStormSpeed,
            'maxWindSpeed': data.maxWindSpeed,
            'closestGround': data.closestGround,
            'minDistance': data.minDistance
        };

        // Safely update each element
        Object.entries(elements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element && value) {
                element.textContent = value;
            }
        });

        // Update charts and tables
        this.updateChart();
        this.updateDistanceChart();
        this.updateActivityTable();
        this.updateTyphoonTrack();

        // Refresh map if it exists
        if (this.map) {
            setTimeout(() => {
                this.map.invalidateSize();
            }, 100);
        }
    }

    setupEventListeners() {
        const typhoonSelect = document.getElementById('typhoonSelect');
        const dateSelect = document.getElementById('dateSelect');
        const backToHomeBtn = document.getElementById('backToHomeBtn');

        // Back to home button
        if (backToHomeBtn) {
            backToHomeBtn.addEventListener('click', () => {
                if (window.pywebview && window.pywebview.api && window.pywebview.api.back_to_welcome) {
                    // Python will handle navigation via evaluate_js
                    window.pywebview.api.back_to_welcome();
                } else {
                    console.warn('Back to welcome API not available');
                }
            });
        }

        if (typhoonSelect) {
            typhoonSelect.addEventListener('change', async (e) => {
                this.currentTyphoon = e.target.value;
                console.log('Switching to typhoon:', this.currentTyphoon);

                // Load new typhoon data
                await this.loadTyphoonData(this.currentTyphoon);

                // Update date selector with available dates for this typhoon
                await this.updateDateSelector();

                // Update display
                this.updateDisplay();
            });
        }

        if (dateSelect) {
            dateSelect.addEventListener('change', async (e) => {
                this.currentDate = e.target.value;
                console.log('Switching to date:', this.currentDate);

                // Reload typhoon data with new date
                await this.loadTyphoonData(this.currentTyphoon);

                // Update display
                this.updateDisplay();
            });
        }
    }

    updateTyphoonSelector() {
        const typhoonSelect = document.getElementById('typhoonSelect');
        if (!typhoonSelect) return;

        // Clear existing options
        typhoonSelect.innerHTML = '';

        // Add typhoon options
        this.typhoonList.forEach(typhoon => {
            const option = document.createElement('option');
            option.value = typhoon.uuid;
            option.textContent = `${typhoon.name} (${typhoon.type})`;
            if (typhoon.uuid === this.currentTyphoon) {
                option.selected = true;
            }
            typhoonSelect.appendChild(option);
        });
    }

    async updateDateSelector() {
        const dateSelect = document.getElementById('dateSelect');
        if (!dateSelect) return;

        // Clear existing options
        dateSelect.innerHTML = '';

        try {
            // Get available dates for current typhoon directly from API
            const dates = await window.pywebview.api.get_typhoon_dates(this.currentTyphoon);
            console.log('Available dates for typhoon:', this.currentTyphoon, dates);

            if (dates && dates.length > 0) {
                dates.forEach(date => {
                    const option = document.createElement('option');
                    option.value = date;
                    option.textContent = date;
                    if (date === this.currentDate) {
                        option.selected = true;
                    }
                    dateSelect.appendChild(option);
                });

                // If no current date is set, use the first available date
                if (!this.currentDate) {
                    this.currentDate = dates[0];
                }
            } else {
                console.warn('No dates available for typhoon:', this.currentTyphoon);
            }
        } catch (error) {
            console.error('Error loading dates:', error);
        }
    }

    onDataUpdate() {
        console.log('Data update notification received');
        // Reload data and refresh display
        this.loadDataFromAPI().then(() => {
            this.updateDisplay();
        });
    }
}

// Initialize the dashboard when the page loads
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing Typhoon Dashboard...');
    window.dashboard = new TyphoonDashboard();
});
