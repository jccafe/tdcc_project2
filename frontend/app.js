const API_BASE = "http://localhost:8001/api";

const screenBtn = document.getElementById('screen-btn');
const updateBtn = document.getElementById('update-btn');
const tableBody = document.getElementById('table-body');
const resultCount = document.getElementById('result-count');
const loadingSpinner = document.getElementById('loading-spinner');
const statusMsg = document.getElementById('status-msg');

function showMessage(msg, type) {
    statusMsg.textContent = msg;
    statusMsg.className = `status-msg msg-${type}`;
    setTimeout(() => {
        statusMsg.textContent = '';
        statusMsg.className = 'status-msg';
    }, 5000);
}

async function loadAvailableDates() {
    const datesEl = document.getElementById('available-dates');
    const startDateSelect = document.getElementById('start-date');
    const endDateSelect = document.getElementById('end-date');
    
    try {
        const res = await fetch(`${API_BASE}/dates`);
        const data = await res.json();
        if (data.status === 'success') {
            if (data.dates.length > 0) {
                // Clear options
                startDateSelect.innerHTML = '';
                endDateSelect.innerHTML = '';
                
                // format dates: YYYYMMDD -> YYYY-MM-DD
                const formattedDates = data.dates.map((d, index) => {
                    const rawDate = d.date;
                    const count = d.count;
                    const fDate = `${rawDate.substring(0,4)}-${rawDate.substring(4,6)}-${rawDate.substring(6,8)}`;
                    
                    const opt1 = document.createElement('option');
                    opt1.value = rawDate;
                    opt1.textContent = fDate;
                    startDateSelect.appendChild(opt1);
                    
                    const opt2 = document.createElement('option');
                    opt2.value = rawDate;
                    opt2.textContent = fDate;
                    endDateSelect.appendChild(opt2);
                    
                    return `<div>${fDate} <span style="opacity: 0.7; font-size: 0.8rem;">(筆數: ${count.toLocaleString()})</span></div>`;
                });
                datesEl.innerHTML = formattedDates.join('');
                
                // Set default selections
                // End date = newest (first in list)
                endDateSelect.selectedIndex = 0;
                // Start date = oldest (last in list)
                startDateSelect.selectedIndex = data.dates.length - 1;
                
            } else {
                datesEl.innerHTML = '無資料';
                startDateSelect.innerHTML = '<option value="">起始週</option>';
                endDateSelect.innerHTML = '<option value="">結束週</option>';
            }
        }
    } catch (err) {
        datesEl.innerHTML = '無法載入已有資料日期';
    }
}

// Load dates on start
loadAvailableDates();

function formatChange(val, isPercent = false) {
    const symbol = val > 0 ? '+' : '';
    const colorClass = val > 0 ? 'positive' : (val < 0 ? 'negative' : 'neutral');
    const unit = isPercent ? '%' : ' 人';
    return `<span class="${colorClass}">${symbol}${val}${unit}</span>`;
}

updateBtn.addEventListener('click', async () => {
    const downloadWeeks = document.getElementById('download-weeks').value;
    updateBtn.disabled = true;
    updateBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> 更新中...';
    
    try {
        const payload = {
            weeks: parseInt(downloadWeeks) || 12
        };
        const res = await fetch(`${API_BASE}/update_data`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        
        if (data.status === 'success' || data.status === 'already_updated') {
            showMessage(`資料更新成功 (${data.date})`, 'success');
            loadAvailableDates();
        } else {
            showMessage('資料更新失敗', 'error');
        }
    } catch (err) {
        showMessage('連線錯誤', 'error');
        console.error(err);
    } finally {
        updateBtn.disabled = false;
        updateBtn.innerHTML = '<i class="fa-solid fa-cloud-arrow-down"></i> 更新 TDCC 資料';
    }
});

screenBtn.addEventListener('click', async () => {
    const retailLevel = document.getElementById('retail-level').value;
    const largeLevel = document.getElementById('large-level').value;
    const weeks = document.getElementById('weeks').value;
    const maDiff = document.getElementById('ma-diff').value;
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;
    
    loadingSpinner.classList.remove('hidden');
    tableBody.innerHTML = '';
    resultCount.textContent = '0';
    
    // Reset progress UI
    const progressBar = document.getElementById('progress-bar');
    const progressPercent = document.getElementById('progress-percent');
    const progressEta = document.getElementById('progress-eta');
    
    progressBar.style.width = '0%';
    progressPercent.textContent = '0%';
    progressEta.textContent = '計算預計時間中...';
    
    // Start polling progress
    let pollInterval = setInterval(async () => {
        try {
            const res = await fetch(`${API_BASE.replace('/api', '')}/api/progress`);
            const data = await res.json();
            
            progressBar.style.width = `${data.percent}%`;
            progressPercent.textContent = `${data.percent}%`;
            
            if (data.eta === -1) {
                progressEta.textContent = '計算預計時間中...';
            } else if (data.eta === 0) {
                progressEta.textContent = '即將完成...';
            } else {
                const mins = Math.floor(data.eta / 60);
                const secs = data.eta % 60;
                progressEta.textContent = `預計剩餘 ${mins > 0 ? mins + ' 分 ' : ''}${secs} 秒`;
            }
        } catch (err) {
            console.error('Progress poll error:', err);
        }
    }, 1000);
    
    try {
        const payload = {
            retail_level: parseInt(retailLevel),
            large_level: parseInt(largeLevel),
            weeks: parseInt(weeks),
            ma_diff_percent: parseFloat(maDiff),
            start_date: startDate || null,
            end_date: endDate || null
        };

        const res = await fetch(`${API_BASE}/screener`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        const data = await res.json();
        
        clearInterval(pollInterval);
        progressBar.style.width = '100%';
        progressPercent.textContent = '100%';
        
        if (data.status === 'success') {
            const results = data.data;
            resultCount.textContent = results.length;
            
            if (results.length === 0) {
                tableBody.innerHTML = `
                    <tr id="empty-state">
                        <td colspan="9" class="empty-message">
                            <div class="empty-icon"><i class="fa-solid fa-ghost"></i></div>
                            <h3>沒有符合條件的標的</h3>
                            <p>嘗試放寬篩選條件，例如減少觀察週數或放寬散戶/大戶定義。</p>
                        </td>
                    </tr>
                `;
            } else {
                results.forEach((stock, index) => {
                    const tr = document.createElement('tr');
                    tr.style.animationDelay = `${index * 0.05}s`; // Staggered animation
                    
                    tr.innerHTML = `
                        <td class="trigger-date" style="font-weight: 500; color: var(--primary);">${stock.trigger_date}</td>
                        <td class="stock-id">${stock.stock_id}</td>
                        <td>${stock.close}</td>
                        <td>${stock.ma20}</td>
                        <td>${formatChange(stock.ma_diff_pct, true)}</td>
                        <td>${stock.retail_current.toLocaleString()}</td>
                        <td>${formatChange(stock.retail_change)}</td>
                        <td>${stock.large_current_pct.toFixed(2)}%</td>
                        <td>${formatChange(stock.large_change_pct, true)}</td>
                    `;
                    tableBody.appendChild(tr);
                });
            }
            showMessage('篩選完成', 'success');
        } else {
            showMessage(data.message || '篩選發生錯誤', 'error');
            tableBody.innerHTML = `<tr><td colspan="9" style="text-align:center;color:#ef4444">${data.message}</td></tr>`;
        }
    } catch (err) {
        showMessage('連線錯誤', 'error');
        console.error(err);
        tableBody.innerHTML = `<tr><td colspan="9" style="text-align:center;color:#ef4444">無法連線至伺服器</td></tr>`;
    } finally {
        clearInterval(pollInterval);
        loadingSpinner.classList.add('hidden');
    }
});
