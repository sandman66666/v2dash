import React, { useState, useEffect } from 'react';
import ReactECharts from 'echarts-for-react';
import { fetchSheetData, getMetricData, TIME_PERIODS } from '../services/googleSheets';

const METRICS = {
  TOTAL_USERS: 'Total Users',
  ACTIVE_USERS: 'Active Users',
  POWER_USERS: 'Power Chat Users',
  REGULAR_USERS: 'Solid Chat Users',
  RENDER_USERS: 'Productions Created',
  SKETCH_USERS: 'Demo Songs Uploaded'
};

const GaugeChart = ({ value, target }) => {
  const min = Math.floor(target * 0.5); // 50% of target as minimum
  const max = target;

  // Calculate exact boundary values
  const redOrangeBoundary = Math.floor(target * 0.75); // 75% of target
  const orangeGreenBoundary = Math.floor(target * 0.9); // 90% of target

  const option = {
    series: [{
      type: 'gauge',
      startAngle: 180,
      endAngle: 0,
      min,
      max,
      radius: '130%',
      center: ['50%', '75%'],
      progress: {
        show: false
      },
      pointer: {
        show: true,
        length: '70%',
        width: 3,
        itemStyle: {
          color: '#303030'
        }
      },
      axisLine: {
        roundCap: false,
        lineStyle: {
          width: 30,
          color: [
            [0.75, '#dc2626'],  // Red section (up to 75%)
            [0.9, '#f59e0b'],   // Orange section (75-90%)
            [1, '#22c55e']      // Green section (90-100%)
          ]
        }
      },
      axisTick: {
        show: false
      },
      splitLine: {
        show: false
      },
      axisLabel: {
        show: false
      },
      anchor: {
        show: false
      },
      title: {
        show: false
      },
      detail: {
        show: false
      },
      data: [{
        value,
        name: 'Progress'
      }]
    }],
    graphic: [
      {
        // 75% marker (red-orange boundary)
        type: 'group',
        left: '28%',
        bottom: '25%',
        children: [
          {
            type: 'line',
            shape: {
              x1: 0,
              y1: -20,
              x2: 0,
              y2: 0
            },
            style: {
              stroke: '#303030',
              lineWidth: 2
            }
          },
          {
            type: 'text',
            left: -15,
            top: -25,
            style: {
              text: redOrangeBoundary.toString(),
              font: '600 14px Inter, sans-serif',
              fill: '#303030'
            }
          }
        ]
      },
      {
        // 90% marker (orange-green boundary)
        type: 'group',
        right: '28%',
        bottom: '25%',
        children: [
          {
            type: 'line',
            shape: {
              x1: 0,
              y1: -20,
              x2: 0,
              y2: 0
            },
            style: {
              stroke: '#303030',
              lineWidth: 2
            }
          },
          {
            type: 'text',
            left: -15,
            top: -25,
            style: {
              text: orangeGreenBoundary.toString(),
              font: '600 14px Inter, sans-serif',
              fill: '#303030'
            }
          }
        ]
      },
      {
        // Target marker (100%)
        type: 'group',
        right: '15%',
        bottom: '25%',
        children: [
          {
            type: 'line',
            shape: {
              x1: 0,
              y1: -20,
              x2: 0,
              y2: 0
            },
            style: {
              stroke: '#303030',
              lineWidth: 2
            }
          },
          {
            type: 'text',
            left: -15,
            top: -25,
            style: {
              text: target.toString(),
              font: '600 14px Inter, sans-serif',
              fill: '#303030'
            }
          }
        ]
      }
    ]
  };

  return (
    <ReactECharts
      option={option}
      style={{ height: '240px', width: '100%', marginBottom: '-40px', marginTop: '-20px' }}
      opts={{ renderer: 'svg' }}
    />
  );
};

const KPICard = ({ value, target, label, timePeriod }) => {
  return (
    <div className="bg-white rounded-lg p-4 shadow-sm">
      <div className="flex justify-between items-start mb-2">
        <span className="text-gray-800 text-xl font-bold">{label}</span>
      </div>
      
      <div className="text-4xl font-semibold text-gray-900 mt-2">
        {value}
      </div>

      <GaugeChart value={value} target={target} />
      
      <div className="mt-1 flex justify-end">
        <span className="text-gray-400 text-xs">{timePeriod}</span>
      </div>
    </div>
  );
};

const TimePeriodDropdown = ({ selectedPeriod, onSelect }) => {
  return (
    <select
      value={selectedPeriod}
      onChange={(e) => onSelect(e.target.value)}
      className="px-3 py-1.5 border rounded-md bg-white shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
    >
      {Object.values(TIME_PERIODS).map((period) => (
        <option key={period} value={period}>
          {period}
        </option>
      ))}
    </select>
  );
};

const KPIDashboard = () => {
  const [selectedPeriod, setSelectedPeriod] = useState(TIME_PERIODS.ALL_TIME);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [lastSuccessfulUpdate, setLastSuccessfulUpdate] = useState(null);

  const metrics = Object.entries(METRICS).map(([key, label]) => ({ key, label }));

  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        const sheetData = await fetchSheetData();
        setData(sheetData);
        const now = new Date();
        setLastUpdate(now.toLocaleString());
        setLastSuccessfulUpdate(now.toLocaleString());
      } catch (err) {
        setError(err.message);
        setLastUpdate(new Date().toLocaleString());
      } finally {
        setLoading(false);
      }
    };

    loadData();
    const interval = setInterval(loadData, 5 * 60 * 1000); // Refresh every 5 minutes

    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="p-6 bg-gray-50 min-h-screen flex items-center justify-center">
        <div className="text-xl text-gray-600">Loading...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 bg-gray-50 min-h-screen flex items-center justify-center">
        <div className="text-xl text-red-600">Error: {error}</div>
      </div>
    );
  }

  return (
    <div className="p-6 bg-gray-50 min-h-screen">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-800">Analytics Dashboard</h1>
          <div className="text-sm text-gray-500 mt-1">
            <div>Last update attempt: {lastUpdate}</div>
            {lastSuccessfulUpdate && (
              <div className="text-green-600">
                Last successful update: {lastSuccessfulUpdate}
              </div>
            )}
          </div>
        </div>
        <TimePeriodDropdown
          selectedPeriod={selectedPeriod}
          onSelect={setSelectedPeriod}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {metrics.map(({ key, label }) => {
          const metricData = getMetricData(data, selectedPeriod, key);
          return (
            <KPICard
              key={key}
              label={label}
              value={metricData.value}
              target={metricData.target}
              timePeriod={selectedPeriod}
            />
          );
        })}
      </div>
    </div>
  );
};

export default KPIDashboard;