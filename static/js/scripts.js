document.addEventListener('DOMContentLoaded', () => {
    // Load data from server
    fetch('/data')
        .then(response => response.json())
        .then(data => {
            populateTable(data);
            createChart(data);
        })
        .catch(error => {
            console.error('Error loading data:', error);
            alert('Failed to load dividend data.');
        });

    // Refresh data button
    document.getElementById('refreshButton').addEventListener('click', refreshData);
    document.getElementById('refreshData').addEventListener('click', refreshData);

    function refreshData() {
        document.getElementById('refreshButton').disabled = true;
        fetch('/refresh')
            .then(response => response.json())
            .then(result => {
                if (result.status === 'success') {
                    // Reload data
                    fetch('/data')
                        .then(response => response.json())
                        .then(data => {
                            populateTable(data);
                            createChart(data);
                            alert('Data refreshed successfully!');
                        });
                } else {
                    alert('Error refreshing data: ' + result.message);
                }
            })
            .catch(error => {
                console.error('Error refreshing data:', error);
                alert('Failed to refresh data.');
            })
            .finally(() => {
                document.getElementById('refreshButton').disabled = false;
            });
    }

    // Populate table
    function populateTable(data) {
        const tbody = document.getElementById('dividendTable');
        tbody.innerHTML = '';
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.Region || 'N/A'}</td>
                <td>${row.Instrument || 'N/A'}</td>
                <td>${row.Symbol || 'N/A'}</td>
                <td>${row.Dividend || 'N/A'}</td>
                <td>${row.Price || 'N/A'}</td>
                <td>${row.Article || 'N/A'}</td>
                <td>${row.Source || 'N/A'}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    // Create bar chart
    function createChart(data) {
        const ctx = document.getElementById('dividendChart').getContext('2d');
        // Destroy existing chart if it exists
        if (window.dividendChart instanceof Chart) {
            window.dividendChart.destroy();
        }
        const instruments = data.map(row => row.Instrument);
        const dividends = data.map(row => {
            const match = row.Dividend?.match(/[\d.]+/);
            return match ? parseFloat(match[0]) : 0;
        });

        window.dividendChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: instruments,
                datasets: [{
                    label: 'Dividend Amount',
                    data: dividends,
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Dividend Amount'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Instrument'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: true
                    }
                }
            }
        });
    }
});
