// Standard time periods without monthly data
export const TIME_PERIODS = {
  ALL_TIME: 'All Time',
  LAST_30_DAYS: 'Last 30 Days',
  LAST_7_DAYS: 'Last 7 Days',
  LAST_3_DAYS: 'Last 3 Days',
  LAST_24_HOURS: 'Last 24 Hours'
};

const BACKEND_URL = 'http://localhost:5001';

// Map frontend metric keys to backend column names
const METRIC_TO_COLUMN_MAP = {
  TOTAL_USERS: 'Total Registered Users',
  ACTIVE_USERS: 'Thread Users (Users who have participated in threads)',
  POWER_USERS: 'Power Users (21+ chat messages - highly engaged)',
  REGULAR_USERS: 'Regular Users (5-20 chat messages - moderately engaged)',
  RENDER_USERS: 'Render Users (Users who have used the render feature)',
  SKETCH_USERS: 'Sketch Users (Users who have created sketches)',
};

// Map frontend metric keys to their target column names in the second sheet
const METRIC_TO_TARGET_MAP = {
  TOTAL_USERS: 'Total Users Target',
  ACTIVE_USERS: 'Active Users Target',
  POWER_USERS: 'Power Users Target',
  REGULAR_USERS: 'Regular Users Target',
  RENDER_USERS: 'Render Users Target',
  SKETCH_USERS: 'Sketch Users Target',
};

export const fetchSheetData = async () => {
  try {
    console.log('Fetching data from backend...');
    const response = await fetch(`${BACKEND_URL}/api/v1/dashboard/metrics`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    
    // Transform the data to match our frontend structure
    const transformedData = {};
    
    // Process each time period
    Object.keys(data).forEach(period => {
      transformedData[period] = {};
      
      // Map each metric to its corresponding value and target
      Object.entries(METRIC_TO_COLUMN_MAP).forEach(([metricKey, columnName]) => {
        // Get the current value
        transformedData[period][metricKey] = data[period][columnName] || 0;
        
        // Get the target value from the targets data
        // If no target is provided, default to null (the component will handle this)
        const targetColumnName = METRIC_TO_TARGET_MAP[metricKey];
        transformedData[period][`${metricKey}_target`] = 
          data.targets?.[targetColumnName] || null;
      });
    });

    return transformedData;
  } catch (error) {
    console.error('Error fetching data:', error);
    throw new Error('Failed to fetch data: ' + error.message);
  }
};

export const getMetricData = (data, timePeriod, metricKey) => {
  if (!data) return { value: 0, target: 0 };

  const periodData = data[timePeriod] || {};
  const value = periodData[metricKey] || 0;
  const target = periodData[`${metricKey}_target`];

  // If no target is provided, default to 20% higher than the current value
  const defaultTarget = Math.ceil(value * 1.2);
  
  return { 
    value, 
    target: target !== null ? target : defaultTarget
  };
};