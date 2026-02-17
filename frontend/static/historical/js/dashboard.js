// Dashboard JavaScript for Typhoon Impact Dashboard

class TyphoonDashboard {
    constructor() {
        this.currentTyphoon = null; // Will be set after data loads
        this.map = null;
        this.charts = {};
        this.typhoonData = {}; // Will be populated from API
        this.fishingGrounds = []; // Will be populated from API
        this.fishingGroundsGeoJSON = null; // GeoJSON polygon data
        this.fishingGroundLayers = []; // Store polygon layers for reference
        this.boatDetectionsLayer = null; // Store boat detections layer
        this.boatDetectionsVisible = true; // Toggle visibility

        this.init();
    }

    async init() {
        try {
            console.log('Starting dashboard initialization...');
            // Bridge console logs to Python if available
            if (window.pywebview && window.pywebview.api && window.pywebview.api.console_log) {
                window.pywebview.api.console_log('info', 'Starting dashboard initialization...');
            }
            await this.loadDashboardData();
            this.setupEventListeners();
            await this.initializeMap();
            this.createCharts();

            // Set default typhoon to first available and populate dropdown
            if (Object.keys(this.typhoonData).length > 0) {
                this.currentTyphoon = Object.keys(this.typhoonData)[0];
                this.populateTyphoonSelector();
                this.setTyphoonSelectorValue(this.currentTyphoon);
                // Update the boat difference chart with the selected typhoon
                this.updateBoatDifferenceChart();
            }

            // Only update display if we have data
            if (this.currentTyphoon && this.typhoonData[this.currentTyphoon]) {
                this.updateDashboard();
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

    async loadDashboardData() {
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

                // Load available years first
                await this.loadAvailableYears();

                // Load dashboard data from the exposed Python API
                const dashboardData = await window.pywebview.api.get_dashboard_data();
                console.log('Dashboard data received:', dashboardData);

                if (dashboardData && dashboardData.typhoons && Object.keys(dashboardData.typhoons).length > 0) {
                    this.typhoonData = dashboardData.typhoons;
                    this.fishingGrounds = dashboardData.fishing_grounds || [];
                    this.fishingGroundsGeoJSON = dashboardData.fishing_grounds_geojson;
                    this.currentYear = dashboardData.latest_year;

                    console.log(`Loaded ${Object.keys(this.typhoonData).length} typhoons`);
                    if (this.fishingGroundsGeoJSON) {
                        console.log(`Loaded fishing grounds GeoJSON with ${this.fishingGroundsGeoJSON.features.length} polygons`);
                    }

                    // Load boat detections for the year
                    if (this.currentYear && window.pywebview.api.get_boat_detections_geojson) {
                        this.loadBoatDetections(this.currentYear);
                    }

                    return; // Success, exit the retry loop
                } else {
                    console.warn('No typhoon data available from API');
                    this.typhoonData = {};
                    this.fishingGrounds = [];
                    this.fishingGroundsGeoJSON = null;
                    return;
                }

            } catch (error) {
                console.error(`Error loading data from API (attempt ${attempt}):`, error);
                if (attempt === 5) {
                    console.error('All attempts failed, setting empty data');
                    this.typhoonData = {};
                    this.fishingGrounds = [];
                    this.fishingGroundsGeoJSON = null;
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second before retry
            }
        }
    }

    setupEventListeners() {
        // Back to home button
        const backToHomeBtn = document.getElementById('backToHomeBtn');
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

        // Set up year selector dropdown
        const yearSelector = document.getElementById('yearSelector');
        if (yearSelector) {
            yearSelector.addEventListener('change', async (e) => {
                const selectedYear = e.target.value;
                if (selectedYear) {
                    await this.loadTyphoonsByYear(parseInt(selectedYear));
                }
            });
        }

        // Set up typhoon selector dropdown
        const typhoonSelector = document.getElementById('typhoonSelector');
        if (typhoonSelector) {
            typhoonSelector.addEventListener('change', (e) => {
                const selectedTyphoon = e.target.value;
                if (selectedTyphoon) {
                    this.currentTyphoon = selectedTyphoon;
                    this.updateBoatDifferenceChart();
                    this.updateSummaryCards();
                }
            });
        }
    }

    initializeMap() {
        // Initialize Leaflet map
        this.map = L.map('map').setView([12.5, 121], 6);

        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(this.map);

        // Add fishing grounds from GeoJSON data if available
        if (this.fishingGroundsGeoJSON && this.fishingGroundsGeoJSON.features) {
            console.log('Rendering fishing ground polygons');

            this.fishingGroundsGeoJSON.features.forEach(feature => {
                const contourId = feature.properties.contour_id;

                // Create polygon layer
                const layer = L.geoJSON(feature, {
                    style: {
                        fillColor: '#4fc3d7',
                        color: '#ffffff',
                        weight: 2,
                        opacity: 1,
                        fillOpacity: 0.20
                    },
                    onEachFeature: (feature, layer) => {
                        // Calculate center of polygon for label
                        const bounds = layer.getBounds();
                        const center = bounds.getCenter();

                        // Add popup
                        layer.bindPopup(`
                            <div class="typhoon-popup">
                                <h4>Ground ${contourId}</h4>
                                <p><span class="popup-label">Center:</span> ${center.lat.toFixed(2)}°, ${center.lng.toFixed(2)}°</p>
                            </div>
                        `);

                        // Add hover effect
                        layer.on('mouseover', function() {
                            this.setStyle({
                                fillOpacity: 0.5
                            });
                        });

                        layer.on('mouseout', function() {
                            this.setStyle({
                                fillOpacity: 0.20
                            });
                        });
                    }
                }).addTo(this.map);

                this.fishingGroundLayers.push(layer);
            });
        } else if (this.fishingGrounds && this.fishingGrounds.length > 0) {
            // Fallback to old circle marker rendering if GeoJSON not available
            console.log('Rendering fishing ground circle markers (fallback)');
            this.fishingGrounds.forEach(ground => {
                L.circleMarker([ground.lat, ground.lng], {
                    radius: 8,
                    fillColor: '#4fc3d7',
                    color: '#fff',
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 0.8
                }).addTo(this.map).bindPopup(`
                    <div class="typhoon-popup">
                        <h4>${ground.name}</h4>
                        <p><span class="popup-label">Location:</span> ${ground.lat.toFixed(2)}°, ${ground.lng.toFixed(2)}°</p>
                    </div>
                `);
            });
        }
    }

    createCharts() {
        this.createBoatDifferenceChart();
        this.createAverageBoatChart();
    }

    createBoatDifferenceChart() {
        const ctx = document.getElementById('boatDifferenceChart');
        if (!ctx) return;

        this.charts.boatDifference = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Ground 0', 'Ground 1', 'Ground 2', 'Ground 3', 'Ground 4'],
                datasets: [
                    {
                        label: 'Baseline',
                        data: [0, 0, 0, 0, 0], // Will be updated with baseline data
                        backgroundColor: '#4fc3d7',
                        borderColor: '#4fc3d7',
                        borderWidth: 1,
                        yAxisID: 'y-right'
                    },
                    {
                        label: '% Difference',
                        data: [0, 0, 0, 0, 0], // Will be updated with difference data
                        backgroundColor: function(context) {
                            const value = context.parsed.y;
                            return value >= 0 ? '#28a745' : '#dc3545';
                        },
                        borderColor: function(context) {
                            const value = context.parsed.y;
                            return value >= 0 ? '#28a745' : '#dc3545';
                        },
                        borderWidth: 1,
                        yAxisID: 'y-left'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        stacked: false
                    },
                    'y-left': {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        title: {
                            display: true,
                            text: '% Difference'
                        },
                        grid: {
                            color: function(context) {
                                if (context.tick.value === 0) {
                                    return '#666'; // Darker line for zero
                                }
                                return '#e0e0e0'; // Light gray for other grid lines
                            },
                            lineWidth: function(context) {
                                if (context.tick.value === 0) {
                                    return 2; // Thicker line for zero
                                }
                                return 1;
                            }
                        },
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    },
                    'y-right': {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: 'Number of Boats'
                        },
                        grid: {
                            drawOnChartArea: false
                        },
                        ticks: {
                            callback: function(value) {
                                return value;
                            }
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
                                const datasetLabel = context.dataset.label;
                                const value = context.parsed.y;
                                if (datasetLabel === 'Baseline') {
                                    return `${datasetLabel}: ${value} boats`;
                                } else {
                                    const direction = value >= 0 ? 'increase' : 'decrease';
                                    return `${datasetLabel}: ${Math.abs(value)}% ${direction}`;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    createAverageBoatChart() {
        const ctx = document.getElementById('averageBoatChart');
        if (!ctx) return;

        this.charts.averageBoat = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [], // Will be populated with typhoon names
                datasets: [{
                    label: 'Average Boats',
                    data: [], // Will be populated with average boat counts
                    backgroundColor: '#9e9e9e',
                    borderColor: '#9e9e9e',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                scales: {
                    x: {
                        beginAtZero: true,
                        max: 400,
                        title: {
                            display: false
                        },
                        grid: {
                            display: true,
                            color: '#e0e0e0'
                        },
                        ticks: {
                            stepSize: 100
                        }
                    },
                    y: {
                        grid: {
                            display: true,
                            color: '#e0e0e0'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                },
                elements: {
                    bar: {
                        barThickness: 'flex',
                        maxBarThickness: 30
                    }
                }
            }
        });
    }

    updateDashboard() {
        const data = this.typhoonData[this.currentTyphoon];
        if (!data) return;

        // Update summary cards
        this.updateSummaryCards();

        // Update boat difference chart
        this.updateBoatDifferenceChart();

        // Update average boat chart
        this.updateAverageBoatChart();

        // Update tables with data from API
        this.updateSpeedTable();
        this.updateDistanceTable();

        // Update table highlights
        this.updateTableHighlights();

        // Update map if needed
        this.updateMap();
    }

    updateSummaryCards() {
        const data = this.typhoonData[this.currentTyphoon];
        if (!data) return;

        // Find most disruptive typhoon (lowest average boat count)
        const mostDisruptive = Object.values(this.typhoonData).reduce((min, typhoon) =>
            typhoon.averageBoats < min.averageBoats ? typhoon : min
        );

        // Find closest typhoon (lowest minimum distance)
        const closest = Object.values(this.typhoonData).reduce((min, typhoon) =>
            parseFloat(typhoon.minDistance) < parseFloat(min.minDistance) ? typhoon : min
        );

        // Find highest storm speed
        const highestSpeed = Object.values(this.typhoonData).reduce((max, typhoon) =>
            parseFloat(typhoon.maxSpeed) > parseFloat(max.maxSpeed) ? typhoon : max
        );

        // Update summary cards
        const summaryCards = document.querySelectorAll('.summary-card');
        if (summaryCards.length >= 3) {
            summaryCards[0].querySelector('.summary-value').textContent = mostDisruptive.name;
            summaryCards[1].querySelector('.summary-value').textContent = closest.name;
            summaryCards[2].querySelector('.summary-value').textContent = `${highestSpeed.maxSpeed} knots`;
        }
    }

    async loadAvailableYears() {
        try {
            console.log('Loading available years...');
            const years = await window.pywebview.api.get_available_years();
            console.log('Available years:', years);

            this.populateYearSelector(years);

            // Set default year to most recent
            if (years.length > 0) {
                const defaultYear = years[years.length - 1];
                this.currentYear = defaultYear;
                this.setYearSelectorValue(defaultYear);
            }
        } catch (error) {
            console.error('Error loading available years:', error);
        }
    }

    populateYearSelector(years) {
        const yearSelector = document.getElementById('yearSelector');
        if (!yearSelector) return;

        // Clear existing options
        yearSelector.innerHTML = '<option value="">Select Year...</option>';

        // Add year options
        years.forEach(year => {
            const option = document.createElement('option');
            option.value = year;
            option.textContent = year.toString();
            yearSelector.appendChild(option);
        });
    }

    setYearSelectorValue(year) {
        const yearSelector = document.getElementById('yearSelector');
        if (yearSelector) {
            yearSelector.value = year;
        }
    }

    async loadTyphoonsByYear(year) {
        try {
            console.log(`Loading typhoons for year ${year}...`);
            const dashboardData = await window.pywebview.api.get_dashboard_data_by_year(year);

            if (dashboardData && dashboardData.typhoons) {
                this.typhoonData = dashboardData.typhoons;
                this.fishingGrounds = dashboardData.fishing_grounds || [];
                this.fishingGroundsGeoJSON = dashboardData.fishing_grounds_geojson;

                // Update current year and map header
                this.currentYear = year;
                this.updateMapHeader();

                console.log(`Loaded ${Object.keys(this.typhoonData).length} typhoons for year ${year}`);
                if (this.fishingGroundsGeoJSON) {
                    console.log(`Loaded fishing grounds GeoJSON with ${this.fishingGroundsGeoJSON.features.length} polygons for year ${year}`);
                }

                // Redraw map with new fishing grounds
                this.updateMapFishingGrounds();

                // Load boat detections for the new year
                if (window.pywebview.api.get_boat_detections_geojson) {
                    this.loadBoatDetections(year);
                }

                // Update typhoon selector
                this.populateTyphoonSelector();

                // Set default typhoon to first available
                if (Object.keys(this.typhoonData).length > 0) {
                    this.currentTyphoon = Object.keys(this.typhoonData)[0];
                    this.setTyphoonSelectorValue(this.currentTyphoon);

                    // Update all dashboard components with the new year's data
                    this.updateBoatDifferenceChart();
                    this.updateSummaryCards();
                    this.updateSpeedTable();
                    this.updateDistanceTable();
                    this.updateAverageBoatChart();
                    this.updateTableHighlights();
                } else {
                    this.currentTyphoon = null;
                    console.warn(`No typhoons available for year ${year}`);
                }
            }
        } catch (error) {
            console.error(`Error loading typhoons for year ${year}:`, error);
        }
    }

    updateMapFishingGrounds() {
        if (!this.map) return;

        // Clear existing fishing ground layers
        this.fishingGroundLayers.forEach(layer => {
            this.map.removeLayer(layer);
        });
        this.fishingGroundLayers = [];

        // Add new fishing grounds from GeoJSON data if available
        if (this.fishingGroundsGeoJSON && this.fishingGroundsGeoJSON.features) {
            console.log('Updating fishing ground polygons');

            this.fishingGroundsGeoJSON.features.forEach(feature => {
                const contourId = feature.properties.contour_id;

                // Create polygon layer
                const layer = L.geoJSON(feature, {
                    style: {
                        fillColor: '#4fc3d7',
                        color: '#ffffff',
                        weight: 2,
                        opacity: 1,
                        fillOpacity: 0.20
                    },
                    onEachFeature: (feature, layer) => {
                        // Calculate center of polygon for label
                        const bounds = layer.getBounds();
                        const center = bounds.getCenter();

                        // Add popup
                        layer.bindPopup(`
                            <div class="typhoon-popup">
                                <h4>Ground ${contourId}</h4>
                                <p><span class="popup-label">Center:</span> ${center.lat.toFixed(2)}°, ${center.lng.toFixed(2)}°</p>
                            </div>
                        `);

                        // Add hover effect
                        layer.on('mouseover', function() {
                            this.setStyle({
                                fillOpacity: 0.5
                            });
                        });

                        layer.on('mouseout', function() {
                            this.setStyle({
                                fillOpacity: 0.20
                            });
                        });
                    }
                }).addTo(this.map);

                this.fishingGroundLayers.push(layer);
            });
        }
    }

    async loadBoatDetections(year) {
        try {
            console.log(`Loading boat detections for year ${year}...`);
            const boatGeoJSON = await window.pywebview.api.get_boat_detections_geojson(year, 5000);

            if (boatGeoJSON && boatGeoJSON.features) {
                console.log(`Loaded ${boatGeoJSON.features.length} boat detection points`);
                this.renderBoatDetections(boatGeoJSON);
            } else {
                console.warn(`No boat detections available for year ${year}`);
            }
        } catch (error) {
            console.error(`Error loading boat detections for year ${year}:`, error);
        }
    }

    renderBoatDetections(boatGeoJSON) {
        if (!this.map) return;

        // Remove existing boat layer if any
        if (this.boatDetectionsLayer) {
            this.map.removeLayer(this.boatDetectionsLayer);
        }

        // Create boat detection layer with small purple circles
        this.boatDetectionsLayer = L.geoJSON(boatGeoJSON, {
            pointToLayer: (feature, latlng) => {
                return L.circleMarker(latlng, {
                    radius: 2,
                    fillColor: '#9333ea',
                    color: '#9333ea',
                    weight: 1,
                    opacity: 0.6,
                    fillOpacity: 0.6
                });
            },
            onEachFeature: (feature, layer) => {
                if (feature.properties.date) {
                    layer.bindPopup(`
                        <div class="typhoon-popup">
                            <h4>Fishing Boat Detection</h4>
                            <p><span class="popup-label">Date:</span> ${feature.properties.date}</p>
                        </div>
                    `);
                }
            }
        });

        // Add to map if visible
        if (this.boatDetectionsVisible) {
            this.boatDetectionsLayer.addTo(this.map);
        }

        console.log('Boat detections rendered on map');
    }

    toggleBoatDetections() {
        if (!this.boatDetectionsLayer) return;

        this.boatDetectionsVisible = !this.boatDetectionsVisible;

        if (this.boatDetectionsVisible) {
            this.boatDetectionsLayer.addTo(this.map);
        } else {
            this.map.removeLayer(this.boatDetectionsLayer);
        }

        // Update toggle button text
        const toggleBtn = document.getElementById('toggleBoatsBtn');
        if (toggleBtn) {
            toggleBtn.textContent = this.boatDetectionsVisible ? 'Hide Boats' : 'Show Boats';
        }
    }

    populateTyphoonSelector() {
        const typhoonSelector = document.getElementById('typhoonSelector');
        if (!typhoonSelector) return;

        // Clear existing options
        typhoonSelector.innerHTML = '<option value="">Select Typhoon...</option>';

        // Add typhoon options
        Object.keys(this.typhoonData).forEach(key => {
            const typhoon = this.typhoonData[key];
            const option = document.createElement('option');
            option.value = key;
            option.textContent = typhoon.name;
            if (key === this.currentTyphoon) {
                option.selected = true;
            }
            typhoonSelector.appendChild(option);
        });
    }

    setTyphoonSelectorValue(typhoonId) {
        const typhoonSelector = document.getElementById('typhoonSelector');
        if (typhoonSelector) {
            typhoonSelector.value = typhoonId;
        }
    }

    updateBoatDifferenceChart() {
        if (!this.charts.boatDifference || !this.currentTyphoon) return;

        const typhoonData = this.typhoonData[this.currentTyphoon];
        if (!typhoonData || !typhoonData.boatData) return;

        const grounds = ['ground0', 'ground1', 'ground2', 'ground3', 'ground4'];
        const baselineData = grounds.map(ground => {
            const groundData = typhoonData.boatData[ground];
            return groundData ? groundData.baseline : 0;
        });
        const differenceData = grounds.map(ground => {
            const groundData = typhoonData.boatData[ground];
            return groundData ? groundData.difference : 0;
        });

        console.log(`Updating boat difference chart for ${typhoonData.name}:`, {
            baseline: baselineData,
            difference: differenceData
        });

        // Update both datasets
        this.charts.boatDifference.data.datasets[0].data = baselineData;  // Baseline
        this.charts.boatDifference.data.datasets[1].data = differenceData; // Difference

        // Update chart scales for dual Y-axes
        const maxBaseline = Math.max(...baselineData);
        const minDifference = Math.min(...differenceData);
        const maxDifference = Math.max(...differenceData);

        // Set right Y-axis (baseline) scale - centered around 0
        this.charts.boatDifference.options.scales['y-right'].max = Math.ceil(maxBaseline * 1.2);
        this.charts.boatDifference.options.scales['y-right'].min = -Math.ceil(maxBaseline * 1.2);

        // Set left Y-axis (% difference) scale - centered around 0
        const absMaxDifference = Math.max(Math.abs(minDifference), Math.abs(maxDifference));
        this.charts.boatDifference.options.scales['y-left'].max = Math.ceil(absMaxDifference * 1.2);
        this.charts.boatDifference.options.scales['y-left'].min = -Math.ceil(absMaxDifference * 1.2);

        this.charts.boatDifference.update();
    }

    updateAverageBoatChart() {
        if (!this.charts.averageBoat) return;

        // Get all typhoon names and average boat counts
        const typhoonNames = Object.keys(this.typhoonData).map(key =>
            this.typhoonData[key].name
        );
        const averageBoats = Object.keys(this.typhoonData).map(key =>
            this.typhoonData[key].averageBoats
        );

        console.log('Updating average boat chart with:', {
            typhoonCount: typhoonNames.length,
            typhoonNames: typhoonNames,
            averageBoats: averageBoats
        });

        // Update chart data
        this.charts.averageBoat.data.labels = typhoonNames;
        this.charts.averageBoat.data.datasets[0].data = averageBoats;

        // Update max scale based on data
        const maxBoats = Math.max(...averageBoats);
        this.charts.averageBoat.options.scales.x.max = Math.ceil(maxBoats * 1.2);

        // Improve spacing and readability
        this.charts.averageBoat.options.scales.y.grid = {
            display: true,
            color: '#e0e0e0'
        };

        // Increase bar thickness for better visibility
        this.charts.averageBoat.options.elements.bar.barThickness = 'flex';
        this.charts.averageBoat.options.elements.bar.maxBarThickness = 30;

        // Ensure all labels are visible
        this.charts.averageBoat.options.scales.y.ticks = {
            autoSkip: false,
            maxRotation: 0
        };

        this.charts.averageBoat.update();
        console.log('Average boat chart updated successfully');
    }

    updateSpeedTable() {
        const tbody = document.querySelector('#speedTable tbody');
        if (!tbody) return;

        // Clear existing rows
        tbody.innerHTML = '';

        // Add rows for each typhoon
        Object.keys(this.typhoonData).forEach(key => {
            const typhoon = this.typhoonData[key];
            const row = document.createElement('tr');
            row.setAttribute('data-typhoon', key);

            // Format numbers to 1 decimal place
            const avgSpeed = parseFloat(typhoon.avgSpeed).toFixed(1);
            const maxSpeed = parseFloat(typhoon.maxSpeed).toFixed(1);

            row.innerHTML = `
                <td class="typhoon-name">${typhoon.name}</td>
                <td>${avgSpeed}</td>
                <td>${maxSpeed}</td>
            `;

            tbody.appendChild(row);
        });
    }

    updateDistanceTable() {
        const tbody = document.querySelector('#distanceTableBottom tbody');
        if (!tbody) return;

        // Clear existing rows
        tbody.innerHTML = '';

        // Add rows for each typhoon
        Object.keys(this.typhoonData).forEach(key => {
            const typhoon = this.typhoonData[key];
            const row = document.createElement('tr');
            row.setAttribute('data-typhoon', key);

            // Get distances from boatData
            const distances = [
                typhoon.boatData.ground0 ? parseFloat(typhoon.boatData.ground0.distance || 0) : 0,
                typhoon.boatData.ground1 ? parseFloat(typhoon.boatData.ground1.distance || 0) : 0,
                typhoon.boatData.ground2 ? parseFloat(typhoon.boatData.ground2.distance || 0) : 0,
                typhoon.boatData.ground3 ? parseFloat(typhoon.boatData.ground3.distance || 0) : 0,
                typhoon.boatData.ground4 ? parseFloat(typhoon.boatData.ground4.distance || 0) : 0
            ];

            row.innerHTML = `
                <td class="typhoon-name">${typhoon.name}</td>
                <td>${distances[0].toFixed(0)}</td>
                <td>${distances[1].toFixed(0)}</td>
                <td>${distances[2].toFixed(0)}</td>
                <td>${distances[3].toFixed(0)}</td>
                <td>${distances[4].toFixed(0)}</td>
            `;

            tbody.appendChild(row);
        });
    }

    updateTableHighlights() {
        // Remove existing highlights
        document.querySelectorAll('.highlighted').forEach(el => {
            el.classList.remove('highlighted');
        });
        document.querySelectorAll('.closest').forEach(el => {
            el.classList.remove('closest');
        });
        document.querySelectorAll('.highest').forEach(el => {
            el.classList.remove('highest');
        });

        // Add highlights for current typhoon
        const currentRows = document.querySelectorAll(`[data-typhoon="${this.currentTyphoon}"]`);
        currentRows.forEach(row => {
            row.classList.add('highlighted');
        });

        // Highlight closest distances in the distance table
        const distanceRows = document.querySelectorAll('#distanceTableBottom tbody tr');
        distanceRows.forEach(row => {
            const cells = row.querySelectorAll('td:not(.typhoon-name)');
            let minDistance = Infinity;
            let closestCell = null;

            cells.forEach((cell, index) => {
                const distance = parseFloat(cell.textContent);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCell = cell;
                }
            });

            if (closestCell) {
                closestCell.classList.add('closest');
            }
        });

        // Highlight highest speeds
        const speedRows = document.querySelectorAll('#speedTable tbody tr');
        let maxAvgSpeed = -Infinity;
        let maxMaxSpeed = -Infinity;
        let maxAvgCell = null;
        let maxMaxCell = null;

        speedRows.forEach(row => {
            const avgCell = row.querySelector('td:nth-child(2)');
            const maxCell = row.querySelector('td:nth-child(3)');

            if (avgCell && maxCell) {
                const avgSpeed = parseFloat(avgCell.textContent);
                const maxSpeed = parseFloat(maxCell.textContent);

                if (avgSpeed > maxAvgSpeed) {
                    maxAvgSpeed = avgSpeed;
                    maxAvgCell = avgCell;
                }

                if (maxSpeed > maxMaxSpeed) {
                    maxMaxSpeed = maxSpeed;
                    maxMaxCell = maxCell;
                }
            }
        });

        if (maxAvgCell) maxAvgCell.classList.add('highest');
        if (maxMaxCell) maxMaxCell.classList.add('highest');
    }

    updateMapHeader() {
        const mapHeader = document.getElementById('mapHeader');
        if (mapHeader && this.currentYear) {
            mapHeader.textContent = `Fishing Grounds in ${this.currentYear}`;
        }
    }

    updateMap() {
        // Update map based on current typhoon selection
        // This could include showing typhoon tracks, updating fishing ground data, etc.
        console.log(`Map updated for typhoon: ${this.currentTyphoon}`);
    }
}



// Initialize the dashboard when the page loads
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing Typhoon Dashboard...');
    window.dashboard = new TyphoonDashboard();
});
