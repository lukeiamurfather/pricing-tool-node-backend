// server.js - Complete Express server implementation with all changes
const express = require('express');
const multer = require('multer');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const parquet = require('parquetjs');
const { promisify } = require('util');
const stream = require('stream');
const pipeline = promisify(stream.pipeline);
const app = express();
const port = process.env.PORT || 3001;

// Configure CORS to allow requests from your React app
app.use(cors());
app.use(express.json());

// Configure file upload storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    // Ensure the uploads directory exists
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) {
      try {
        fs.mkdirSync(uploadDir, { recursive: true });
        console.log(`Created upload directory: ${uploadDir}`);
      } catch (err) {
        console.error(`Failed to create upload directory: ${err.message}`);
        return cb(new Error(`Could not create upload directory: ${err.message}`));
      }
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    // Generate a more robust unique filename
    const timestamp = Date.now();
    const safeFilename = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
    cb(null, `${timestamp}-${safeFilename}`);
  }
});

// Improve multer configuration - removing file size limits
const upload = multer({ 
  storage,
  limits: { 
    // Remove the fileSize limit
    files: 1 // Only allow one file per upload
  },
  fileFilter: (req, file, cb) => {
    // Only allow CSV and parquet files
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext !== '.csv' && ext !== '.parquet') {
      return cb(new Error('Only CSV and Parquet files are allowed'));
    }
    cb(null, true);
  }
});

// Improve session management
const fileSessions = new Map();

// Add a function to debug sessions
function logActiveSessions() {
  console.log(`Active sessions (${fileSessions.size}):`);
  fileSessions.forEach((session, id) => {
    console.log(`- ${id}: ${session.fileType}, ${session.filePath}`);
  });
}

app.get('/api/check-session/:fileId', (req, res) => {
  const { fileId } = req.params;
  
  if (!fileId) {
    return res.status(400).json({ valid: false, error: 'No fileId provided' });
  }
  
  const sessionExists = fileSessions.has(fileId);
  let fileExists = false;
  let filePath = null;
  
  if (sessionExists) {
    const session = fileSessions.get(fileId);
    filePath = session.filePath;
    fileExists = fs.existsSync(filePath);
  }
  
  res.json({
    valid: sessionExists && fileExists,
    sessionExists,
    fileExists,
    filePath: sessionExists ? path.basename(filePath) : null
  });
});

// Add a more robust session cleanup function
function cleanupSessions() {
  const now = Date.now();
  const maxAge = 24 * 60 * 60 * 1000; // 24 hours
  const initialCount = fileSessions.size;
  
  console.log(`Running session cleanup... (current count: ${initialCount})`);
  
  for (const [fileId, session] of fileSessions.entries()) {
    try {
      // Extract timestamp from fileId
      const timestamp = parseInt(fileId.split('-')[0]);
      
      // Check if session is expired
      if (now - timestamp > maxAge) {
        console.log(`Session ${fileId} expired (age: ${((now - timestamp) / 3600000).toFixed(1)} hours)`);
        
        // Remove temporary file
        if (session.filePath && fs.existsSync(session.filePath)) {
          try {
            fs.unlinkSync(session.filePath);
            console.log(`Deleted temp file ${session.filePath}`);
          } catch (error) {
            console.error(`Error deleting temp file ${session.filePath}:`, error);
          }
        }
        
        // Remove session
        fileSessions.delete(fileId);
      }
    } catch (error) {
      console.error(`Error processing session ${fileId} during cleanup:`, error);
    }
  }
  
  const removedCount = initialCount - fileSessions.size;
  console.log(`Session cleanup complete: removed ${removedCount} expired sessions, ${fileSessions.size} remaining`);
}

// Add this function near the cleanupSessions function
function cleanupAllFiles() {
  console.log("Cleaning up all files from uploads and datasets directories...");
  
  // Clean uploads directory
  const uploadsDir = path.join(__dirname, 'uploads');
  if (fs.existsSync(uploadsDir)) {
    try {
      const files = fs.readdirSync(uploadsDir);
      let filesDeleted = 0;
      
      for (const file of files) {
        const filePath = path.join(uploadsDir, file);
        try {
          fs.unlinkSync(filePath);
          filesDeleted++;
        } catch (err) {
          console.error(`Failed to delete file ${filePath}:`, err);
        }
      }
      
      console.log(`Cleaned up ${filesDeleted} files from uploads directory`);
    } catch (err) {
      console.error("Error cleaning uploads directory:", err);
    }
  }
  
  // Clean datasets directory
  const datasetsDir = path.join(__dirname, 'datasets');
  if (fs.existsSync(datasetsDir)) {
    try {
      const directories = fs.readdirSync(datasetsDir);
      let directoriesDeleted = 0;
      
      for (const dir of directories) {
        const dirPath = path.join(datasetsDir, dir);
        try {
          // Check if it's a directory
          if (fs.statSync(dirPath).isDirectory()) {
            // Delete all files in the directory
            const dirFiles = fs.readdirSync(dirPath);
            for (const file of dirFiles) {
              fs.unlinkSync(path.join(dirPath, file));
            }
            // Delete the directory itself
            fs.rmdirSync(dirPath);
            directoriesDeleted++;
          } else {
            // If it's a file, just delete it
            fs.unlinkSync(dirPath);
            directoriesDeleted++;
          }
        } catch (err) {
          console.error(`Failed to delete directory ${dirPath}:`, err);
        }
      }
      
      console.log(`Cleaned up ${directoriesDeleted} directories/files from datasets directory`);
    } catch (err) {
      console.error("Error cleaning datasets directory:", err);
    }
  }
  
  // Clear sessions map
  fileSessions.clear();
  console.log("Cleared all file sessions");
}

// Run cleanup every hour and log active sessions
setInterval(() => {
  cleanupSessions();
  logActiveSessions();
}, 60 * 60 * 1000);

// Run initial cleanup and log at startup
process.nextTick(() => {
  cleanupSessions();
  logActiveSessions();
});

// Add this utility function near the top of your server.js file

/**
 * Transform a data row by applying field mappings and derived field transformations
 * @param {Object} row - Original data row
 * @param {Object} fieldAssignments - Mapping from standard field names to data field names
 * @param {Object} derivedFields - Configuration for derived fields
 * @param {boolean} standardizeNames - Whether to rename fields to standard names
 * @returns {Object} - Transformed row
 */

function applyFieldTransformations(row, fieldAssignments, derivedFields = {}, standardizeNames = true) {
  // Step 1: Create a copy of the row to avoid modifying the original
  let transformedRow = { ...row };
  
  // Step 2: Apply derived field transformations
  if (derivedFields && Object.keys(derivedFields).length > 0) {
    Object.entries(derivedFields).forEach(([fieldName, config]) => {
      if (!config || !config.type) {
        console.warn(`Invalid derived field configuration for ${fieldName}`);
        return;
      }
      
      if (config.type === 'capped') {
        if (!config.sourceField) {
          console.warn(`Missing sourceField for capped variable ${fieldName}`);
          return;
        }
        
        const sourceValue = parseFloat(row[config.sourceField]);
        transformedRow[fieldName] = isNaN(sourceValue) ? null : Math.min(sourceValue, config.threshold);
      } 
      else if (config.type === 'banded') {
        if (!config.sourceField || !config.bands || !Array.isArray(config.bands)) {
          console.warn(`Invalid configuration for banded variable ${fieldName}`);
          return;
        }
        
        const sourceValue = parseFloat(row[config.sourceField]);
        
        if (isNaN(sourceValue)) {
          transformedRow[fieldName] = null;
          return;
        }
        
        // Find first matching band (important for overlapping bands)
        let bandValue = null;
        for (const band of config.bands) {
          if (!band || band.min === undefined || band.max === undefined) {
            console.warn(`Invalid band in ${fieldName}: ${JSON.stringify(band)}`);
            continue;
          }
          
          const min = Number(band.min);
          const max = Number(band.max);
          
          // Check if the value falls within this band's range
          if (sourceValue >= min && sourceValue <= max) {
            bandValue = band.name;
            break; // Stop at the first matching band
          }
        }
        
        transformedRow[fieldName] = bandValue || 'Unassigned';
      }
    });
  }
  
  // Step 3: Apply field standardization if requested
  if (standardizeNames && fieldAssignments) {
    const standardizedRow = {};
    
    // Standard field mappings
    const mappings = {
      'Claim Count': 'count',
      'Ultimate Claims': 'guc',
      'Exposure': 'exposure',
      'Year': 'year'
    };
    
    // Add standardized fields first
    Object.entries(mappings).forEach(([standardKey, standardField]) => {
      const originalField = fieldAssignments[standardKey];
      if (originalField && transformedRow[originalField] !== undefined) {
        let value = transformedRow[originalField];
        
        // Convert to appropriate types
        if (standardField === 'year') {
          value = parseInt(value) || 0;
        } else {
          value = parseFloat(value) || 0;
        }
        
        standardizedRow[standardField] = value;
      } else {
        // Provide default values for missing fields
        standardizedRow[standardField] = standardField === 'year' ? 0 : 0.0;
      }
    });
    
    // Copy all other fields
    Object.entries(transformedRow).forEach(([field, value]) => {
      // Skip fields that have already been standardized
      const skipField = Object.values(fieldAssignments).includes(field);
      if (!skipField) {
        standardizedRow[field] = value;
      }
    });
    
    // Return the standardized row
    return standardizedRow;
  }
  
  // If not standardizing, just return the row with derived fields
  return transformedRow;
}

app.use(express.json({ limit: '5000mb' }));
app.use(express.urlencoded({ extended: true, limit: '5000mb' }));

// Upload file to server
app.post('/api/upload', (req, res) => {
  console.log('File upload request received');
  
  // Use a single-function middleware to catch multer errors
  upload.single('file')(req, res, (err) => {
    if (err) {
      if (err instanceof multer.MulterError) {
        // A Multer error occurred when uploading
        console.error('Multer error during upload:', err);
        return res.status(400).json({ 
          error: `Upload error: ${err.message}`,
          code: err.code 
        });
      } else {
        // An unknown error occurred
        console.error('Unknown error during upload:', err);
        return res.status(500).json({ 
          error: 'Failed to upload file',
          message: err.message 
        });
      }
    }
    
    // If we get here, upload succeeded but we still need to check if we got a file
    if (!req.file) {
      console.error('No file received in upload request');
      return res.status(400).json({ error: 'No file uploaded' });
    }
    
    // Process the uploaded file
    const filePath = req.file.path;
    const fileId = req.file.filename;
    const fileExtension = path.extname(req.file.originalname).toLowerCase();
    const fileSize = req.file.size;
    
    console.log(`File uploaded successfully: ${fileId} (${fileExtension}), size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`File saved to: ${filePath}`);
    
    try {
      // Create a session for this file
      fileSessions.set(fileId, {
        filePath,
        fileType: fileExtension,
        metadata: null,
        processedData: null,
        fileSize: fileSize
      });
      
      console.log(`Created session for fileId: ${fileId}`);
      console.log(`Current sessions: ${Array.from(fileSessions.keys()).join(', ')}`);
      
      // For very large files, consider returning a response immediately
      // and processing the file asynchronously
      if (fileSize > 100 * 1024 * 1024) { // For files larger than 100MB
        console.log(`Large file detected (${(fileSize / 1024 / 1024).toFixed(2)} MB). Starting async processing.`);
        
        // Return a response that the file was received and is being processed
        res.json({
          fileId,
          status: 'processing',
          message: 'Large file is being processed in the background',
          fileSize: fileSize
        });
        
        // Start analyzing the file asynchronously
        analyzeFile(filePath, fileExtension)
          .then(result => {
            console.log(`Background processing complete for ${fileId}`);
            if (fileSessions.has(fileId)) {
              fileSessions.get(fileId).metadata = result;
            }
          })
          .catch(error => {
            console.error(`Error analyzing file ${fileId} in background:`, error);
          });
          
        return;
      }
      
      // For smaller files, process synchronously
      analyzeFile(filePath, fileExtension)
        .then(result => {
          // Make sure session still exists before updating it
          if (fileSessions.has(fileId)) {
            console.log(`Updating session metadata for ${fileId}`);
            fileSessions.get(fileId).metadata = result;
            
            // Send the response with file details
            res.json({
              fileId,
              stats: result.stats,
              totalRows: result.totalRows,
              previewData: result.previewData
            });
          } else {
            console.error(`Session for ${fileId} no longer exists after analysis`);
            res.status(500).json({ 
              error: 'Session lost during processing',
              message: 'The server lost track of your upload during processing. Please try again.'
            });
          }
        })
        .catch(error => {
          console.error(`Error analyzing file ${fileId}:`, error);
          res.status(500).json({ 
            error: 'Failed to analyze file',
            message: error.message
          });
        });
    } catch (error) {
      console.error('Error processing uploaded file:', error);
      res.status(500).json({ 
        error: 'Failed to process the file',
        message: error.message 
      });
    }
  });
});

// Add a status check endpoint for large file processing
app.get('/api/file-status/:fileId', (req, res) => {
  const { fileId } = req.params;
  
  if (!fileId) {
    return res.status(400).json({ error: 'No fileId provided' });
  }
  
  // Check if session exists
  if (!fileSessions.has(fileId)) {
    return res.status(404).json({ 
      status: 'not_found',
      error: 'File session not found' 
    });
  }
  
  const session = fileSessions.get(fileId);
  
  // Check if file exists
  if (!fs.existsSync(session.filePath)) {
    return res.status(404).json({
      status: 'file_missing',
      error: 'File no longer exists on the server'
    });
  }
  
  // Check if metadata is available (processing complete)
  if (session.metadata) {
    return res.json({
      status: 'complete',
      stats: session.metadata.stats,
      totalRows: session.metadata.totalRows,
      fileSize: session.fileSize
    });
  }
  
  // If we get here, file is still being processed
  return res.json({
    status: 'processing',
    message: 'File is still being processed',
    fileSize: session.fileSize
  });
});

// Analyze a file to get metadata without loading all data
async function analyzeFile(filePath, fileExtension) {
  console.log(`Analyzing file: ${filePath} (${fileExtension})`);
  
  // First check if file exists
  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }
  
  // Check file extension
  if (fileExtension === '.csv') {
    console.log('Processing as CSV file');
    return analyzeCSV(filePath);
  } else if (fileExtension === '.parquet') {
    console.log('Processing as Parquet file');
    return analyzeParquet(filePath);
  } else {
    throw new Error(`Unsupported file format: ${fileExtension}`);
  }
}

// Make analyzeCSV more robust
async function analyzeCSV(filePath) {
  console.log(`Analyzing CSV file: ${filePath}`);
  
  return new Promise((resolve, reject) => {
    const stats = {};
    const previewData = [];
    let totalRows = 0;
    let headers = null;
    let processedRows = 0;
    const startTime = Date.now();
    
    // Check file size first
    try {
      const fileSize = fs.statSync(filePath).size;
      console.log(`CSV file size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
    } catch (err) {
      console.error(`Error checking file size: ${err.message}`);
    }

    // Process all rows for accurate statistics, but only save a preview
    const stream = fs.createReadStream(filePath)
      .on('error', (err) => {
        console.error(`Error reading CSV file: ${err.message}`);
        reject(new Error(`Failed to read CSV file: ${err.message}`));
      })
      .pipe(csv({
        skipEmptyLines: true,
        delimiter: [',', '\t', '|', ';'] // Try multiple delimiters
      }))
      .on('headers', (headerList) => {
        console.log(`CSV headers detected: ${headerList.join(', ')}`);
        headers = headerList;
        // Initialize field stats
        headers.forEach(field => {
          stats[field] = {
            fieldType: 'string', // Default, will be refined
            uniqueValues: new Set(),
            nullCount: 0,
            sampleValues: []
          };
        });
      })
      .on('data', (row) => {
        processedRows++;
        totalRows++;
        
        // Only collect preview for first 1000 rows
        if (previewData.length < 1000) {
          previewData.push(row);
        }
        
        // Process statistics for ALL rows
        Object.entries(row).forEach(([field, value]) => {
          if (!stats[field]) {
            // Handle new fields that weren't in the headers
            stats[field] = {
              fieldType: 'string',
              uniqueValues: new Set(),
              nullCount: 0,
              sampleValues: []
            };
          }
          
          // Count null/empty values
          if (value === null || value === '' || value === undefined) {
            stats[field].nullCount++;
            return;
          }
          
          // Add unique values
          stats[field].uniqueValues.add(value);
          
          // Collect sample values
          if (stats[field].sampleValues.length < 50) {
            stats[field].sampleValues.push(value);
          }
          
          // Detect field type
          if (stats[field].fieldType !== 'string') {
            // Already determined to be a number, no need to check again
            return;
          }
          
          // Check if value is number
          const isNumber = !isNaN(Number(value)) && value !== '';
          if (isNumber) {
            if (String(value).includes('.')) {
              stats[field].fieldType = 'numeric';
            } else {
              stats[field].fieldType = 'integer';
            }
          }
        });
        
        // Log progress for large files
        if (processedRows % 100000 === 0) {
          const elapsed = (Date.now() - startTime) / 1000;
          const rowsPerSecond = processedRows / elapsed;
          console.log(`Processed ${processedRows.toLocaleString()} rows in ${elapsed.toFixed(1)}s (${Math.round(rowsPerSecond).toLocaleString()} rows/sec)`);
        }
      })
      .on('end', () => {
        // Calculate percentages and finalize stats
        Object.keys(stats).forEach(field => {
          stats[field].nullPercentage = ((stats[field].nullCount / totalRows) * 100).toFixed(2);
          stats[field].uniqueValues = stats[field].uniqueValues.size;
        });
        
        const elapsed = (Date.now() - startTime) / 1000;
        console.log(`Completed CSV analysis: ${totalRows.toLocaleString()} rows in ${elapsed.toFixed(1)}s`);
        
        resolve({
          stats,
          totalRows,
          headers,
          previewData
        });
      })
      .on('error', (err) => {
        console.error(`Error processing CSV: ${err.message}`);
        reject(new Error(`Failed to process CSV: ${err.message}`));
      });
    
    // Add timeout for CSV processing
    const timeout = setTimeout(() => {
      console.error('CSV analysis timed out after 5 minutes');
      stream.destroy();
      reject(new Error('CSV analysis timed out after 5 minutes'));
    }, 5 * 60 * 1000); // 5 minute timeout
    
    // Clear timeout when done
    stream.on('end', () => clearTimeout(timeout));
    stream.on('error', () => clearTimeout(timeout));
  });
}

// Analyze parquet file to extract metadata
async function analyzeParquet(filePath) {
  // Parquet analysis is more complex - this is a simplified version
  try {
    const reader = await parquet.ParquetReader.openFile(filePath);
    const metadata = await reader.getMetadata();
    const schema = reader.getSchema();
    
    const stats = {};
    const headers = Object.keys(schema.fields);
    const previewData = [];
    
    // Get total rows
    const totalRows = metadata.rows;
    
    // Get preview rows (limited to 1000)
    const cursor = reader.getCursor();
    for (let i = 0; i < Math.min(1000, totalRows); i++) {
      const row = await cursor.next();
      if (row) previewData.push(row);
    }
    
    // To properly compute stats for ALL rows (not just preview)
    // we need to read through the entire file
    const fullCursor = reader.getCursor();
    for (const field of headers) {
      stats[field] = {
        fieldType: 'string', // Default
        uniqueValues: new Set(),
        nullCount: 0,
        sampleValues: [],
      };
    }
    
    let row;
    let rowCount = 0;
    while ((row = await fullCursor.next()) !== null) {
      rowCount++;
      
      for (const [field, value] of Object.entries(row)) {
        if (!stats[field]) continue;
        
        // Count null values
        if (value === null || value === undefined || value === '') {
          stats[field].nullCount++;
          continue;
        }
        
        // Track unique values
        stats[field].uniqueValues.add(String(value));
        
        // Collect sample values
        if (stats[field].sampleValues.length < 50) {
          stats[field].sampleValues.push(value);
        }
        
        // Detect field type
        if (stats[field].fieldType !== 'string') {
          continue; // Already determined
        }
        
        // Try to infer type
        const isNumber = !isNaN(Number(value)) && value !== '';
        if (isNumber) {
          if (String(value).includes('.')) {
            stats[field].fieldType = 'numeric';
          } else {
            stats[field].fieldType = 'integer';
          }
        }
      }
    }
    
    // Finalize stats
    for (const field of headers) {
      stats[field].nullPercentage = ((stats[field].nullCount / rowCount) * 100).toFixed(2);
      stats[field].uniqueValues = stats[field].uniqueValues.size;
    }
    
    await reader.close();
    return { 
      stats, 
      totalRows, 
      headers, 
      previewData 
    };
  } catch (error) {
    console.error('Error analyzing parquet file:', error);
    throw error;
  }
}

function applyFieldTransformations(row, fieldAssignments, derivedFields = {}, standardizeNames = true) {
  // Step 1: Create a copy of the row to avoid modifying the original
  let transformedRow = { ...row };
  
  // Step 2: Apply derived field transformations
  if (derivedFields && Object.keys(derivedFields).length > 0) {
    Object.entries(derivedFields).forEach(([fieldName, config]) => {
      if (!config || !config.type) {
        console.warn(`Invalid derived field configuration for ${fieldName}`);
        return;
      }
      
      if (config.type === 'capped') {
        if (!config.sourceField) {
          console.warn(`Missing sourceField for capped variable ${fieldName}`);
          return;
        }
        
        // Make sure we can find the source field (either directly or through a mapping)
        let sourceValue;
        
        // Check if the sourceField is a standard field name
        if (['count', 'guc', 'exposure', 'year'].includes(config.sourceField) && fieldAssignments) {
          const standardMapping = {
            'count': fieldAssignments['Claim Count'],
            'guc': fieldAssignments['Ultimate Claims'],
            'exposure': fieldAssignments['Exposure'],
            'year': fieldAssignments['Year']
          };
          
          if (standardMapping[config.sourceField]) {
            // Use the mapped field to get the source value
            sourceValue = row[standardMapping[config.sourceField]];
            console.log(`Using mapped field ${standardMapping[config.sourceField]} for standardized source ${config.sourceField}`);
          } else {
            // Fall back to the original field name
            sourceValue = row[config.sourceField];
          }
        } else {
          // Use the source field directly
          sourceValue = row[config.sourceField];
        }
        
        // Parse the value to a number
        if (sourceValue !== undefined) {
          if (typeof sourceValue === 'string') {
            sourceValue = parseFloat(sourceValue.replace(/,/g, ''));
          } else if (typeof sourceValue !== 'number') {
            sourceValue = null;
          }
        } else {
          sourceValue = null;
        }
        
        // Apply the cap if we have a valid source value
        if (sourceValue === null || isNaN(sourceValue)) {
          transformedRow[fieldName] = null;
        } else {
          transformedRow[fieldName] = Math.min(sourceValue, config.threshold);
        }
      } 
      else if (config.type === 'banded') {
        // Existing banded field code remains the same
        if (!config.sourceField || !config.bands || !Array.isArray(config.bands)) {
          console.warn(`Invalid configuration for banded variable ${fieldName}`);
          return;
        }
        
        let sourceValue;
        
        // Check if the sourceField is a standard field name
        if (['count', 'guc', 'exposure', 'year'].includes(config.sourceField) && fieldAssignments) {
          const standardMapping = {
            'count': fieldAssignments['Claim Count'],
            'guc': fieldAssignments['Ultimate Claims'],
            'exposure': fieldAssignments['Exposure'],
            'year': fieldAssignments['Year']
          };
          
          if (standardMapping[config.sourceField]) {
            // Use the mapped field to get the source value
            sourceValue = row[standardMapping[config.sourceField]];
          } else {
            // Fall back to the original field name
            sourceValue = row[config.sourceField];
          }
        } else {
          // Use the source field directly
          sourceValue = row[config.sourceField];
        }
        
        // Parse the value to a number for banded fields
        if (sourceValue !== undefined) {
          if (typeof sourceValue === 'string') {
            sourceValue = parseFloat(sourceValue.replace(/,/g, ''));
          } else if (typeof sourceValue !== 'number') {
            sourceValue = null;
          }
        } else {
          sourceValue = null;
        }
        
        if (sourceValue === null || isNaN(sourceValue)) {
          transformedRow[fieldName] = null;
          return;
        }
        
        // Find first matching band (important for overlapping bands)
        let bandValue = null;
        for (const band of config.bands) {
          if (!band || band.min === undefined || band.max === undefined) {
            console.warn(`Invalid band in ${fieldName}: ${JSON.stringify(band)}`);
            continue;
          }
          
          const min = Number(band.min);
          const max = Number(band.max);
          
          // Check if the value falls within this band's range
          if (sourceValue >= min && sourceValue <= max) {
            bandValue = band.name;
            break; // Stop at the first matching band
          }
        }
        
        transformedRow[fieldName] = bandValue || 'Unassigned';
      }
    });
  }
  
  // Step 3: Apply field standardization if requested
  if (standardizeNames && fieldAssignments) {
    const standardizedRow = {};
    
    // Standard field mappings
    const mappings = {
      'Claim Count': 'count',
      'Ultimate Claims': 'guc',
      'Exposure': 'exposure',
      'Year': 'year'
    };
    
    // Add standardized fields first
    Object.entries(mappings).forEach(([standardKey, standardField]) => {
      const originalField = fieldAssignments[standardKey];
      if (originalField && transformedRow[originalField] !== undefined) {
        let value = transformedRow[originalField];
        
        // Convert to appropriate types
        if (standardField === 'year') {
          value = parseInt(value) || 0;
        } else {
          value = parseFloat(value) || 0;
        }
        
        standardizedRow[standardField] = value;
      } else {
        // Provide default values for missing fields
        standardizedRow[standardField] = standardField === 'year' ? 0 : 0.0;
      }
    });
    
    // Copy all other fields, including derived fields
    Object.entries(transformedRow).forEach(([field, value]) => {
      // Skip fields that have already been standardized
      const skipField = Object.values(fieldAssignments).includes(field);
      
      // If it's a derived field, always include it
      const isDerivedField = derivedFields && Object.keys(derivedFields).includes(field);
      
      if (isDerivedField || !skipField) {
        standardizedRow[field] = value;
      }
    });
    
    // Return the standardized row
    return standardizedRow;
  }
  
  // If not standardizing, just return the row with derived fields
  return transformedRow;
}

// Select fields to include
app.post('/api/select-fields', express.json(), (req, res) => {
  try {
    const { fileId, selectedFields } = req.body;
    console.log(`Field selection request for fileId: ${fileId}`);
    console.log('Selected fields:', selectedFields);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Update session with selected fields
    session.selectedFields = selectedFields;
    console.log(`Updated session.selectedFields to include ${selectedFields.length} fields`);
    
    // Return updated field stats for selected fields only
    const filteredStats = {};
    for (const field of selectedFields) {
      if (session.metadata && session.metadata.stats && session.metadata.stats[field]) {
        filteredStats[field] = session.metadata.stats[field];
      }
    }
    
    console.log(`Filtered stats to ${Object.keys(filteredStats).length} fields`);
    
    res.json({
      stats: filteredStats,
      totalRows: session.metadata ? session.metadata.totalRows : 0
    });
  } catch (error) {
    console.error('Error selecting fields:', error);
    res.status(500).json({ error: 'Failed to process field selection: ' + error.message });
  }
});

// Assign required fields
app.post('/api/assign-fields', express.json(), async (req, res) => {
  try {
    const { fileId, fieldAssignments } = req.body;
    console.log(`Field assignment request for fileId: ${fileId}`);
    console.log('Field assignments:', fieldAssignments);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Update session with field assignments
    session.fieldAssignments = fieldAssignments;
    
    // Create standardized field mappings for easier access
    const fieldMappings = {
      count: fieldAssignments['Claim Count'],
      guc: fieldAssignments['Ultimate Claims'],
      exposure: fieldAssignments['Exposure'],
      year: fieldAssignments['Year']
    };
    
    // Store the mappings in the session
    session.fieldMappings = fieldMappings;
    
    console.log('Updated field mappings:', fieldMappings);
    
    // Extract unique years if year field is assigned
    let availableYears = [];
    if (fieldAssignments['Year']) {
      const yearField = fieldAssignments['Year'];
      console.log(`Extracting years using field: ${yearField}`);
      
      if (session.fileType === '.csv') {
        // For CSV, scan the entire file to get all unique years
        availableYears = await extractUniqueYearsFromCSV(session.filePath, yearField);
      } else if (session.metadata && session.metadata.previewData && session.metadata.previewData.length > 0) {
        // Fallback to preview data for non-CSV or if full scan fails
        const years = session.metadata.previewData
          .map(row => row[yearField])
          .filter(year => year !== null && year !== undefined && !isNaN(year))
          .map(year => Number(year));
        availableYears = [...new Set(years)].sort((a, b) => a - b);
      }
      
      console.log(`Available years: ${availableYears.length > 0 ? availableYears.join(', ') : 'None found'}`);
    }
    
    res.json({
      success: true,
      availableYears,
      fieldMappings
    });
  } catch (error) {
    console.error('Error assigning fields:', error);
    res.status(500).json({ error: 'Failed to process field assignment: ' + error.message });
  }
});

// Extract all unique years from a CSV file
function extractUniqueYearsFromCSV(filePath, yearField) {
  console.log(`Extracting unique years from CSV using field: ${yearField}`);
  return new Promise((resolve, reject) => {
    const years = new Set();
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        const year = row[yearField];
        if (year !== null && year !== undefined && !isNaN(year)) {
          years.add(Number(year));
        }
      })
      .on('end', () => {
        const uniqueYears = [...years].sort((a, b) => a - b);
        console.log(`Found ${uniqueYears.length} unique years: ${uniqueYears.join(', ')}`);
        resolve(uniqueYears);
      })
      .on('error', (err) => {
        console.error(`Error extracting years: ${err.message}`);
        reject(err);
      });
  });
}

// Filter data by year
app.post('/api/filter-by-year', express.json(), async (req, res) => {
  try {
    const { fileId, selectedYears } = req.body;
    console.log(`Year filtering request for fileId: ${fileId}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Update session with selected years
    session.selectedYears = selectedYears;
    
    // Get the year field from assignments
    const yearField = session.fieldAssignments['Year'];
    if (!yearField) {
      return res.status(400).json({ error: 'Year field not assigned' });
    }
    
    console.log(`Using year field: ${yearField}, selected years:`, selectedYears);
    
    // Get the Ultimate Claims field
    const gucField = session.fieldAssignments['Ultimate Claims'];
    
    // Count records by year (using full dataset scan)
    const recordsByYear = await countRecordsByYear(
      session.filePath, 
      session.fileType, 
      yearField, 
      gucField
    );
    
    console.log('Records by year:', recordsByYear);
    
    res.json({
      success: true,
      recordsByYear
    });
  } catch (error) {
    console.error('Error filtering by year:', error);
    res.status(500).json({ error: 'Failed to filter by year: ' + error.message });
  }
});

// Count records by year
async function countRecordsByYear(filePath, fileType, yearField, gucField) {
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      const recordsByYear = {};
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          const year = Number(row[yearField]);
          if (isNaN(year)) return;
          
          if (!recordsByYear[year]) {
            recordsByYear[year] = { count: 0, totalGUC: 0 };
          }
          
          recordsByYear[year].count++;
          if (gucField && !isNaN(Number(row[gucField]))) {
            recordsByYear[year].totalGUC += Number(row[gucField]);
          }
        })
        .on('end', () => {
          resolve(recordsByYear);
        })
        .on('error', reject);
    });
  } else if (fileType === '.parquet') {
    // Simplified parquet implementation
    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      const recordsByYear = {};
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        const year = Number(row[yearField]);
        if (isNaN(year)) continue;
        
        if (!recordsByYear[year]) {
          recordsByYear[year] = { count: 0, totalGUC: 0 };
        }
        
        recordsByYear[year].count++;
        if (gucField && !isNaN(Number(row[gucField]))) {
          recordsByYear[year].totalGUC += Number(row[gucField]);
        }
      }
      
      await reader.close();
      return recordsByYear;
    } catch (error) {
      console.error('Error counting records by year:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

// Apply data filters
app.post('/api/apply-filters', express.json(), async (req, res) => {
  try {
    const { fileId, filters, minExposureValue, fieldAssignments, selectedFields } = req.body;
    console.log(`Apply filters request for fileId: ${fileId}`);
    console.log('Filters:', filters);
    console.log('Min exposure value:', minExposureValue);
    console.log('Selected fields:', selectedFields);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Update session with filters
    session.filters = filters;
    session.minExposureValue = minExposureValue || 0;
    
    // Save selected fields if provided
    if (selectedFields) {
      session.selectedFields = selectedFields;
    }
    
    // Count rows that would be retained after filtering
    const filteredStats = await calculateFilteredStats(
      session.filePath,
      session.fileType,
      fieldAssignments || session.fieldAssignments,
      filters,
      minExposureValue,
      session.selectedYears,
      session.selectedFields // Pass selected fields to the function
    );
    
    console.log(`Filtered from ${filteredStats.totalRows} to ${filteredStats.filteredRows} rows`);
    
    res.json({
      filteredRows: filteredStats.filteredRows,
      totalRows: filteredStats.totalRows,
      stats: filteredStats.stats
    });
  } catch (error) {
    console.error('Error applying filters:', error);
    res.status(500).json({ error: 'Failed to apply filters: ' + error.message });
  }
});

// Calculate filtered statistics
async function calculateFilteredStats(filePath, fileType, fieldAssignments, filters, minExposureValue, selectedYears, selectedFields) {
  console.log('Calculating filtered stats with field assignments:', fieldAssignments);
  console.log('Selected fields:', selectedFields);
  
  // Map standard field names to actual field names
  const exposureField = fieldAssignments['Exposure'];
  const gucField = fieldAssignments['Ultimate Claims'];
  const countField = fieldAssignments['Claim Count'];
  const yearField = fieldAssignments['Year'];
  
  // Create list of original fields that should be excluded (they're mapped to standard fields)
  const originalMappedFields = [exposureField, gucField, countField, yearField];
  console.log('Original mapped fields to exclude:', originalMappedFields);
  
  // Create reverse mapping for easier reference
  const reverseMapping = {
    [exposureField]: 'exposure',
    [gucField]: 'guc',
    [countField]: 'count',
    [yearField]: 'year'
  };
  
  console.log('Field mappings:', { exposure: exposureField, guc: gucField, count: countField, year: yearField });
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      let totalRows = 0;
      let filteredRows = 0;
      const fieldStats = {};
      
      // Add standard field stats - these are the standardized field names
      fieldStats['count'] = {
        fieldType: 'numeric',
        uniqueValues: new Set(),
        nullCount: 0,
        originalField: countField
      };
      
      fieldStats['guc'] = {
        fieldType: 'numeric',
        uniqueValues: new Set(),
        nullCount: 0,
        originalField: gucField
      };
      
      fieldStats['exposure'] = {
        fieldType: 'numeric',
        uniqueValues: new Set(),
        nullCount: 0,
        originalField: exposureField
      };
      
      fieldStats['year'] = {
        fieldType: 'integer',
        uniqueValues: new Set(),
        nullCount: 0,
        originalField: yearField
      };
      
      // Initialize stats for selected fields if provided
      if (selectedFields && selectedFields.length > 0) {
        selectedFields.forEach(field => {
          // Skip original fields that are mapped to standard fields
          if (originalMappedFields.includes(field)) {
            return;
          }
          
          if (!fieldStats[field]) {
            fieldStats[field] = {
              fieldType: 'string', // Default, will be refined
              uniqueValues: new Set(),
              nullCount: 0
            };
          }
        });
      }
      
      // Process the CSV to calculate stats
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          totalRows++;
          
          // Apply filters
          // Year filter
          const year = Number(row[yearField]);
          if (selectedYears && !selectedYears[year]) return;
          
          const exposure = parseFloat(row[exposureField]) || 0;
          const guc = parseFloat(row[gucField]) || 0;
          const count = parseFloat(row[countField]) || 0;
          
          // Check filter conditions
          let passesFilters = true;
          
          if (filters.exposuresGreaterThan && exposure <= minExposureValue) {
            passesFilters = false;
          }
          
          if (filters.exposureLessThanOrEqualToOne && exposure > 1) {
            passesFilters = false;
          }
          
          if (filters.ultimateClaimsNonNegative && guc < 0) {
            passesFilters = false;
          }
          
          if (filters.claimCountNonNegative && count < 0) {
            passesFilters = false;
          }
          
          if (passesFilters) {
            filteredRows++;
            
            // Process standardized fields
            fieldStats['count'].uniqueValues.add(count.toString());
            fieldStats['guc'].uniqueValues.add(guc.toString());
            fieldStats['exposure'].uniqueValues.add(exposure.toString());
            fieldStats['year'].uniqueValues.add(year.toString());
            
            if (count === null || count === undefined || count === '') fieldStats['count'].nullCount++;
            if (guc === null || guc === undefined || guc === '') fieldStats['guc'].nullCount++;
            if (exposure === null || exposure === undefined || exposure === '') fieldStats['exposure'].nullCount++;
            if (year === null || year === undefined || year === '') fieldStats['year'].nullCount++;
            
            // Only process selected fields if provided
            if (selectedFields && selectedFields.length > 0) {
              selectedFields.forEach(field => {
                // Skip original fields that are mapped to standard fields
                if (originalMappedFields.includes(field)) {
                  return;
                }
                
                if (field in row && field in fieldStats) {
                  const value = row[field];
                  
                  // Determine field type
                  if (fieldStats[field].fieldType === 'string' && !isNaN(Number(value))) {
                    fieldStats[field].fieldType = value.includes('.') ? 'numeric' : 'integer';
                  }
                  
                  if (value === null || value === '' || value === undefined) {
                    fieldStats[field].nullCount++;
                  } else {
                    fieldStats[field].uniqueValues.add(value);
                  }
                }
              });
            }
          }
        })
        .on('end', () => {
          // Finalize statistics
          Object.keys(fieldStats).forEach(field => {
            if (fieldStats[field].uniqueValues instanceof Set) {
              fieldStats[field].uniqueValues = fieldStats[field].uniqueValues.size;
            }
            fieldStats[field].nullPercentage = ((fieldStats[field].nullCount / Math.max(filteredRows, 1)) * 100).toFixed(2);
          });
          
          resolve({
            totalRows,
            filteredRows,
            stats: fieldStats
          });
        })
        .on('error', (error) => {
          console.error('Error processing CSV for filtered stats:', error);
          reject(error);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files...
    // (similar changes as for CSV implementation)
  }
  
  throw new Error('Unsupported file type');
}

// Complete calculateFieldStats function
async function calculateFieldStats(filePath, fileType, fieldName, derivedFieldConfig) {
  console.log(`Calculating stats for field ${fieldName}, derived config:`, derivedFieldConfig);
  
  // Function to get derived field value
  const getDerivedValue = (row, config) => {
    if (!config) return null;
    
    if (config.type === 'capped') {
      const sourceField = config.sourceField;
      const sourceValue = row[sourceField];
      
      if (sourceValue === null || sourceValue === undefined || sourceValue === '') {
        return null;
      }
      
      let parsedValue;
      if (typeof sourceValue === 'number') {
        parsedValue = sourceValue;
      } else if (typeof sourceValue === 'string') {
        parsedValue = parseFloat(sourceValue.replace(/,/g, ''));
      } else {
        return null;
      }
      
      if (isNaN(parsedValue) || !isFinite(parsedValue)) {
        return null;
      }
      
      return Math.min(parsedValue, config.threshold);
    } 
    else if (config.type === 'banded') {
      const sourceField = config.sourceField;
      const bands = config.bands;
      const sourceValue = row[sourceField];
      
      if (sourceValue === null || sourceValue === undefined || sourceValue === '') {
        return null;
      }
      
      let parsedValue;
      if (typeof sourceValue === 'number') {
        parsedValue = sourceValue;
      } else if (typeof sourceValue === 'string') {
        parsedValue = parseFloat(sourceValue.replace(/,/g, ''));
      } else {
        return null;
      }
      
      if (isNaN(parsedValue) || !isFinite(parsedValue)) {
        return null;
      }
      
      // Find first matching band
      for (const band of bands) {
        if (!band || band.min === undefined || band.max === undefined) {
          console.warn(`Invalid band configuration: ${JSON.stringify(band)}`);
          continue;
        }
        
        const min = Number(band.min);
        const max = Number(band.max);
        
        if (parsedValue >= min && parsedValue <= max) {
          return band.name;
        }
      }
      
      return 'Unassigned';
    }
    
    return null;
  };
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      const uniqueValues = new Set();
      let nullCount = 0;
      let totalCount = 0;
      let validValueCount = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          totalCount++;
          
          let value;
          if (derivedFieldConfig) {
            // Calculate the derived field value
            value = getDerivedValue(row, derivedFieldConfig);
          } else {
            // Get regular field value
            value = row[fieldName];
          }
          
          if (value === null || value === undefined || value === '') {
            nullCount++;
            return;
          }
          
          validValueCount++;
          uniqueValues.add(String(value));
        })
        .on('end', () => {
          // Determine field type
          let fieldType;
          
          if (derivedFieldConfig) {
            // Use the configured type for derived fields
            if (derivedFieldConfig.type === 'capped') {
              fieldType = 'numeric';
            } else if (derivedFieldConfig.type === 'banded') {
              fieldType = 'string';
            } else {
              fieldType = 'string'; // Default for unknown derived types
            }
          } else {
            // Try to infer type for non-derived fields
            fieldType = 'string'; // Default type
            
            // Get a sample of values to determine type
            const values = Array.from(uniqueValues);
            const sampleSize = Math.min(values.length, 10);
            const sample = values.slice(0, sampleSize);
            
            // Check if all samples are numeric
            const allNumeric = sample.every(val => {
              const num = parseFloat(val);
              return !isNaN(num) && isFinite(num);
            });
            
            if (allNumeric) {
              // Check if any have decimal points
              const hasDecimals = sample.some(val => val.includes('.'));
              fieldType = hasDecimals ? 'numeric' : 'integer';
            }
          }
          
          const stats = {
            fieldType,
            uniqueValues: uniqueValues.size,
            nullCount,
            nullPercentage: ((nullCount / Math.max(totalCount, 1)) * 100).toFixed(2)
          };
          
          console.log(`Stats for ${fieldName}: ${totalCount} total rows, ${validValueCount} valid values, ${uniqueValues.size} unique values`);
          resolve(stats);
        })
        .on('error', (err) => {
          console.error(`Error processing CSV for field stats ${fieldName}:`, err);
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files
    try {
      console.log(`Opening parquet file for field stats ${fieldName}: ${filePath}`);
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      const uniqueValues = new Set();
      let nullCount = 0;
      let totalCount = 0;
      let validValueCount = 0;
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        totalCount++;
        
        let value;
        if (derivedFieldConfig) {
          // Calculate the derived field value
          value = getDerivedValue(row, derivedFieldConfig);
        } else {
          // Get regular field value
          value = row[fieldName];
        }
        
        if (value === null || value === undefined || value === '') {
          nullCount++;
          continue;
        }
        
        validValueCount++;
        uniqueValues.add(String(value));
      }
      
      await reader.close();
      
      // Determine field type
      let fieldType;
      
      if (derivedFieldConfig) {
        // Use the configured type for derived fields
        if (derivedFieldConfig.type === 'capped') {
          fieldType = 'numeric';
        } else if (derivedFieldConfig.type === 'banded') {
          fieldType = 'string';
        } else {
          fieldType = 'string'; // Default for unknown derived types
        }
      } else {
        // Try to infer type for non-derived fields
        fieldType = 'string'; // Default type
        
        // Get a sample of values to determine type
        const values = Array.from(uniqueValues);
        const sampleSize = Math.min(values.length, 10);
        const sample = values.slice(0, sampleSize);
        
        // Check if all samples are numeric
        const allNumeric = sample.every(val => {
          const num = parseFloat(val);
          return !isNaN(num) && isFinite(num);
        });
        
        if (allNumeric) {
          // Check if any have decimal points
          const hasDecimals = sample.some(val => val.includes('.'));
          fieldType = hasDecimals ? 'numeric' : 'integer';
        }
      }
      
      const stats = {
        fieldType,
        uniqueValues: uniqueValues.size,
        nullCount,
        nullPercentage: ((nullCount / Math.max(totalCount, 1)) * 100).toFixed(2)
      };
      
      console.log(`Stats for ${fieldName} from parquet: ${totalCount} total rows, ${validValueCount} valid values, ${uniqueValues.size} unique values`);
      return stats;
    } catch (error) {
      console.error(`Error calculating field stats for Parquet file ${fieldName}:`, error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

// Train/test split - returns statistics only
app.post('/api/train-test-split', express.json(), async (req, res) => {
  try {
    const { fileId, trainPercentage } = req.body;
    console.log(`Train/test split request for fileId: ${fileId}, trainPercentage: ${trainPercentage}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Update session with training percentage
    session.trainPercentage = trainPercentage;
    
    console.log('Field assignments for train/test split:', session.fieldAssignments);
    
    // Calculate statistics for the split without actually storing all data
    const trainTestStats = await calculateTrainTestStats(
      session.filePath,
      session.fileType,
      session.fieldAssignments,
      session.filters,
      session.minExposureValue,
      session.selectedYears,
      trainPercentage
    );
    
    console.log('Train/test split results:', {
      trainSize: trainTestStats.trainSize,
      testSize: trainTestStats.testSize,
      trainPercentage: trainTestStats.trainPercentage,
      testPercentage: trainTestStats.testPercentage,
    });
    
    // For preview purposes, create sample data from the full dataset
    // This is just for UI display, not for the actual model training
    const previewTrainData = [];
    const previewTestData = [];
    
    // Check if we have filtered data or should use raw preview data
    if (session.filteredData && session.filteredData.length > 0) {
      console.log(`Using filtered data (${session.filteredData.length} rows) for preview`);
      // Use filtered data for preview
      const filteredSample = session.filteredData.slice(0, Math.min(20, session.filteredData.length));
      
      // Split sample data according to train percentage
      const trainCount = Math.ceil(filteredSample.length * (trainPercentage / 100));
      for (let i = 0; i < filteredSample.length; i++) {
        if (i < trainCount) {
          previewTrainData.push(filteredSample[i]);
        } else {
          previewTestData.push(filteredSample[i]);
        }
      }
    } else if (session.metadata && session.metadata.previewData) {
      console.log('Using preview data for sample split');
      // Use the preview data to create preview train/test splits
      const previewSample = session.metadata.previewData.slice(0, Math.min(20, session.metadata.previewData.length));
      
      // Add standardized field names to preview data
      const transformedPreview = previewSample.map(row => 
        applyFieldTransformations(row, session.fieldAssignments, session.derivedFields, true)
      );
      
      // Split the preview data for display
      const trainCount = Math.ceil(transformedPreview.length * (trainPercentage / 100));
      for (let i = 0; i < transformedPreview.length; i++) {
        if (i < trainCount) {
          previewTrainData.push(transformedPreview[i]);
        } else {
          previewTestData.push(transformedPreview[i]);
        }
      }
    }
    
    console.log(`Created preview data: ${previewTrainData.length} train samples, ${previewTestData.length} test samples`);
    if (previewTrainData.length > 0) {
      console.log('Sample train data (first row):', previewTrainData[0]);
    }
    
    // Add the full statistics and sample data to the response
    res.json({
      ...trainTestStats,
      trainData: previewTrainData,  // Just for UI preview
      testData: previewTestData,    // Just for UI preview
      message: `Full split complete: ${trainTestStats.trainSize} train rows, ${trainTestStats.testSize} test rows`
    });
  } catch (error) {
    console.error('Error calculating train/test split:', error);
    res.status(500).json({ error: 'Failed to calculate train/test split: ' + error.message });
  }
});

// Calculate statistics for train/test split
async function calculateTrainTestStats(filePath, fileType, fieldAssignments, filters, minExposureValue, selectedYears, trainPercentage) {
  console.log('Calculating train/test statistics with:');
  console.log('- Field assignments:', fieldAssignments);
  console.log('- Train percentage:', trainPercentage);
  
  // Field mappings from standard names to actual data fields
  const exposureField = fieldAssignments['Exposure'];
  const gucField = fieldAssignments['Ultimate Claims'];
  const countField = fieldAssignments['Claim Count'];
  const yearField = fieldAssignments['Year'];
  
  console.log(`Using field mappings: exposure=${exposureField}, guc=${gucField}, count=${countField}, year=${yearField}`);
  
  // Statistics to collect
  const trainStats = { count: 0, guc: { sum: 0, sumSquared: 0 }, claimCount: { sum: 0, sumSquared: 0 } };
  const testStats = { count: 0, guc: { sum: 0, sumSquared: 0 }, claimCount: { sum: 0, sumSquared: 0 } };
  
  // Random seed for consistent splitting
  const seed = 12345;
  const random = (min, max) => {
    const x = Math.sin(seed + filteredRows) * 10000;
    const r = x - Math.floor(x);
    return min + r * (max - min);
  };
  
  // Function to determine if row passes filters
  const passesFilters = (row) => {
    // Year filter
    const year = Number(row[yearField]);
    if (selectedYears && !selectedYears[year]) return false;
    
    // Data filters
    const exposure = parseFloat(row[exposureField]) || 0;
    const guc = parseFloat(row[gucField]) || 0;
    const count = parseFloat(row[countField]) || 0;
    
    if (filters && filters.exposuresGreaterThan && exposure <= minExposureValue) return false;
    if (filters && filters.exposureLessThanOrEqualToOne && exposure > 1) return false;
    if (filters && filters.ultimateClaimsNonNegative && guc < 0) return false;
    if (filters && filters.claimCountNonNegative && count < 0) return false;
    
    return true;
  };
  
  // Initialize variables to track row counts
  let totalRows = 0;
  let filteredRows = 0;
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // First pass: count rows that will be included (after filtering)
      let totalFilteredRows = 0;
      
      // Create a readstream to count filtered rows
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          totalRows++;
          
          if (passesFilters(row)) {
            totalFilteredRows++;
          }
        })
        .on('end', () => {
          console.log(`Found ${totalFilteredRows} rows after filtering from ${totalRows} total rows`);
          
          // Now that we know the total number of filtered rows, do a second pass to collect statistics
          console.log('Starting second pass to collect statistics...');
          
          fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
              filteredRows++;
              
              if (!passesFilters(row)) return;
              
              const guc = parseFloat(row[gucField]) || 0;
              const count = parseFloat(row[countField]) || 0;
              
              // Determine train/test split using a persistent seeded randomization to ensure consistency
              // This ensures that the same rows go to the same split each time the function is called
              // We use the row index (filteredRows) as part of the seed
              const randomValue = random(0, 100);
              const isTrainRow = randomValue < trainPercentage;
              
              if (isTrainRow) {
                trainStats.count++;
                trainStats.guc.sum += guc;
                trainStats.guc.sumSquared += guc * guc;
                trainStats.claimCount.sum += count;
                trainStats.claimCount.sumSquared += count * count;
              } else {
                testStats.count++;
                testStats.guc.sum += guc;
                testStats.guc.sumSquared += guc * guc;
                testStats.claimCount.sum += count;
                testStats.claimCount.sumSquared += count * count;
              }
              
              // Log progress periodically
              if (filteredRows % 100000 === 0) {
                console.log(`Processed ${filteredRows} rows in second pass`);
              }
            })
            .on('end', () => {
              // Calculate final statistics
              const trainGucMean = trainStats.count > 0 ? trainStats.guc.sum / trainStats.count : 0;
              const trainGucVariance = trainStats.count > 0 ? 
                (trainStats.guc.sumSquared / trainStats.count) - (trainGucMean * trainGucMean) : 0;
                
              const trainCountMean = trainStats.count > 0 ? trainStats.claimCount.sum / trainStats.count : 0;
              const trainCountVariance = trainStats.count > 0 ? 
                (trainStats.claimCount.sumSquared / trainStats.count) - (trainCountMean * trainCountMean) : 0;
                
              const testGucMean = testStats.count > 0 ? testStats.guc.sum / testStats.count : 0;
              const testGucVariance = testStats.count > 0 ? 
                (testStats.guc.sumSquared / testStats.count) - (testGucMean * testGucMean) : 0;
                
              const testCountMean = testStats.count > 0 ? testStats.claimCount.sum / testStats.count : 0;
              const testCountVariance = testStats.count > 0 ? 
                (testStats.claimCount.sumSquared / testStats.count) - (testCountMean * testCountMean) : 0;
              
              console.log(`Stats calculation complete: ${totalRows} total rows, ${totalFilteredRows} filtered rows`);
              console.log(`Train: ${trainStats.count} rows, Test: ${testStats.count} rows`);
              console.log(`Train GUC mean: ${trainGucMean.toFixed(2)}, Test GUC mean: ${testGucMean.toFixed(2)}`);
              
              resolve({
                trainSize: trainStats.count,
                testSize: testStats.count,
                totalRows: totalRows,
                filteredRows: totalFilteredRows,
                trainPercentage,
                testPercentage: 100 - trainPercentage,
                trainStats: {
                  guc: {
                    mean: trainGucMean.toFixed(2),
                    variance: trainGucVariance.toFixed(2)
                  },
                  count: {
                    mean: trainCountMean.toFixed(2),
                    variance: trainCountVariance.toFixed(2)
                  }
                },
                testStats: {
                  guc: {
                    mean: testGucMean.toFixed(2),
                    variance: testGucVariance.toFixed(2)
                  },
                  count: {
                    mean: testCountMean.toFixed(2),
                    variance: testCountVariance.toFixed(2)
                  }
                }
              });
            })
            .on('error', (err) => {
              console.error('Error calculating train/test stats (second pass):', err);
              reject(err);
            });
        })
        .on('error', (err) => {
          console.error('Error calculating filtered row count (first pass):', err);
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Similar implementation for Parquet files...
    try {
      console.log('Processing Parquet file for train/test stats');
      
      // First, count total filtered rows
      const reader1 = await parquet.ParquetReader.openFile(filePath);
      const cursor1 = reader1.getCursor();
      
      let totalRows = 0;
      let totalFilteredRows = 0;
      
      let row = null;
      while ((row = await cursor1.next()) !== null) {
        totalRows++;
        
        if (passesFilters(row)) {
          totalFilteredRows++;
        }
      }
      
      await reader1.close();
      console.log(`Found ${totalFilteredRows} rows after filtering from ${totalRows} total rows`);
      
      // Second pass to collect statistics
      const reader2 = await parquet.ParquetReader.openFile(filePath);
      const cursor2 = reader2.getCursor();
      
      let filteredRows = 0;
      
      row = null;
      while ((row = await cursor2.next()) !== null) {
        filteredRows++;
        
        if (!passesFilters(row)) continue;
        
        const guc = parseFloat(row[gucField]) || 0;
        const count = parseFloat(row[countField]) || 0;
        
        // Use same seeded randomization for consistency
        const randomValue = random(0, 100);
        const isTrainRow = randomValue < trainPercentage;
        
        if (isTrainRow) {
          trainStats.count++;
          trainStats.guc.sum += guc;
          trainStats.guc.sumSquared += guc * guc;
          trainStats.claimCount.sum += count;
          trainStats.claimCount.sumSquared += count * count;
        } else {
          testStats.count++;
          testStats.guc.sum += guc;
          testStats.guc.sumSquared += guc * guc;
          testStats.claimCount.sum += count;
          testStats.claimCount.sumSquared += count * count;
        }
        
        // Log progress periodically
        if (filteredRows % 100000 === 0) {
          console.log(`Processed ${filteredRows} Parquet rows`);
        }
      }
      
      await reader2.close();
      
      // Calculate final statistics (same as CSV implementation)
      const trainGucMean = trainStats.count > 0 ? trainStats.guc.sum / trainStats.count : 0;
      const trainGucVariance = trainStats.count > 0 ? 
        (trainStats.guc.sumSquared / trainStats.count) - (trainGucMean * trainGucMean) : 0;
        
      const trainCountMean = trainStats.count > 0 ? trainStats.claimCount.sum / trainStats.count : 0;
      const trainCountVariance = trainStats.count > 0 ? 
        (trainStats.claimCount.sumSquared / trainStats.count) - (trainCountMean * trainCountMean) : 0;
        
      const testGucMean = testStats.count > 0 ? testStats.guc.sum / testStats.count : 0;
      const testGucVariance = testStats.count > 0 ? 
        (testStats.guc.sumSquared / testStats.count) - (testGucMean * testGucMean) : 0;
        
      const testCountMean = testStats.count > 0 ? testStats.claimCount.sum / testStats.count : 0;
      const testCountVariance = testStats.count > 0 ? 
        (testStats.claimCount.sumSquared / testStats.count) - (testCountMean * testCountMean) : 0;
      
      console.log(`Parquet stats calculation complete: ${totalRows} total rows, ${totalFilteredRows} filtered rows`);
      console.log(`Train: ${trainStats.count} rows, Test: ${testStats.count} rows`);
      
      return {
        trainSize: trainStats.count,
        testSize: testStats.count,
        totalRows: totalRows,
        filteredRows: totalFilteredRows,
        trainPercentage,
        testPercentage: 100 - trainPercentage,
        trainStats: {
          guc: {
            mean: trainGucMean.toFixed(2),
            variance: trainGucVariance.toFixed(2)
          },
          count: {
            mean: trainCountMean.toFixed(2),
            variance: trainCountVariance.toFixed(2)
          }
        },
        testStats: {
          guc: {
            mean: testGucMean.toFixed(2),
            variance: testGucVariance.toFixed(2)
          },
          count: {
            mean: testCountMean.toFixed(2),
            variance: testCountVariance.toFixed(2)
          }
        }
      };
    } catch (error) {
      console.error('Error processing Parquet train/test stats:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

// Finalize and send data to model
app.post('/api/finalize-data', express.json(), async (req, res) => {
  try {
    const { fileId, fieldAssignments, trainPercentage } = req.body;
    console.log(`Finalize data request for fileId: ${fileId}, trainPercentage: ${trainPercentage || 70}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Get the selected fields from the session
    const selectedFields = session.selectedFields || [];
    console.log('Selected fields for finalization:', selectedFields);
    
    // Generate a processed dataset ID
    const datasetId = `dataset_${Date.now()}`;
    const datasetsDir = path.join(__dirname, 'datasets');
    
    // Create dataset directory
    const datasetDir = path.join(datasetsDir, datasetId);
    if (!fs.existsSync(datasetDir)) {
      fs.mkdirSync(datasetDir, { recursive: true });
    }
    
    // Create standardized mappings for use in the model
    const fieldMappings = {
      count: session.fieldAssignments['Claim Count'],
      guc: session.fieldAssignments['Ultimate Claims'],
      exposure: session.fieldAssignments['Exposure'],
      year: session.fieldAssignments['Year']
    };
    
    console.log('Field mappings for finalization:', fieldMappings);
    
    // Get train/test stats if available, otherwise calculate them
    let trainTestStats = null;
    if (session.trainSize && session.testSize) {
      console.log('Using cached train/test stats');
      trainTestStats = {
        trainSize: session.trainSize,
        testSize: session.testSize,
        trainStats: session.trainStats,
        testStats: session.testStats
      };
    } else {
      console.log('Calculating train/test stats');
      trainTestStats = await calculateTrainTestStats(
        session.filePath,
        session.fileType,
        session.fieldAssignments,
        session.filters,
        session.minExposureValue,
        session.selectedYears,
        trainPercentage || 70
      );
      
      // Cache the results
      session.trainSize = trainTestStats.trainSize;
      session.testSize = trainTestStats.testSize;
      session.trainStats = trainTestStats.trainStats;
      session.testStats = trainTestStats.testStats;
    }
    
    // Store the finalized configuration
    const finalizedConfig = {
      id: datasetId,
      fileId,
      fieldAssignments: session.fieldAssignments,
      fieldMappings: fieldMappings,
      selectedFields: selectedFields, // Add selected fields to config
      filters: session.filters,
      minExposureValue: session.minExposureValue,
      selectedYears: session.selectedYears,
      trainPercentage: trainPercentage || 70,
      filePath: session.filePath,
      fileType: session.fileType,
      derivedFields: session.derivedFields || {},  // Include derived fields in config
      trainSize: trainTestStats.trainSize,
      testSize: trainTestStats.testSize,
      trainStats: trainTestStats.trainStats,
      testStats: trainTestStats.testStats
    };
    
    // Save configuration
    fs.writeFileSync(
      path.join(datasetDir, 'config.json'),
      JSON.stringify(finalizedConfig, null, 2)
    );
    
    console.log('Creating train/test split files with standardized field names');
    
    // Actually create the train/test split files with standardized field names
    // Pass the derivedFields parameter to ensure derived fields are included
    const splitResult = await createTrainTestSplitFiles(
      session.filePath, 
      session.fileType,
      session.fieldAssignments, 
      session.filters || {}, 
      session.minExposureValue || 0,
      session.selectedYears || {},
      trainPercentage || 70,
      session.derivedFields || {},  // Pass derived fields to split function
      path.join(datasetDir, 'train.csv'),
      path.join(datasetDir, 'test.csv'),
      selectedFields // Pass selected fields to filter the data
    );
    
    // Load a sample of the train/test data to send back to the client
    // This helps ensure the client has some data to work with
    const trainSample = [];
    const testSample = [];
    
    try {
      // Read a sample of each file (up to 100 rows)
      const trainPath = path.join(datasetDir, 'train.csv');
      const testPath = path.join(datasetDir, 'test.csv');
      
      if (fs.existsSync(trainPath)) {
        console.log('Loading train sample from CSV');
        let rowCount = 0;
        fs.createReadStream(trainPath)
          .pipe(csv())
          .on('data', (row) => {
            if (rowCount < 100) {
              trainSample.push(row);
              rowCount++;
            }
          })
          .on('end', () => {
            console.log(`Loaded ${trainSample.length} rows from train sample`);
          });
      }
      
      if (fs.existsSync(testPath)) {
        console.log('Loading test sample from CSV');
        let rowCount = 0;
        fs.createReadStream(testPath)
          .pipe(csv())
          .on('data', (row) => {
            if (rowCount < 100) {
              testSample.push(row);
              rowCount++;
            }
          })
          .on('end', () => {
            console.log(`Loaded ${testSample.length} rows from test sample`);
          });
      }
    } catch (error) {
      console.warn('Error loading train/test samples:', error);
      // Continue without samples - not critical
    }
    
    // Wait a moment to make sure file reading is complete
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Return the complete information including samples of the data and derived fields
    res.json({
      success: true,
      datasetId,
      fieldMappings, // Return the standardized mappings to the client
      derivedFields: session.derivedFields || {}, // Include derived fields in response
      trainSize: trainTestStats.trainSize,
      testSize: trainTestStats.testSize,
      trainStats: trainTestStats.trainStats,
      testStats: trainTestStats.testStats,
      trainData: trainSample,  // Include a sample of the train data
      testData: testSample,    // Include a sample of the test data
      message: 'Data configuration saved and ready for modeling'
    });
  } catch (error) {
    console.error('Error finalizing data:', error);
    res.status(500).json({ error: 'Failed to finalize data: ' + error.message });
  }
});

// Function to create actual train/test split files
async function createTrainTestSplitFiles(
  filePath, 
  fileType, 
  fieldAssignments, 
  filters, 
  minExposureValue,
  selectedYears,
  trainPercentage,
  derivedFields,
  trainOutputPath,
  testOutputPath,
  selectedFields // Add this parameter
) {
  console.log('Creating train/test split files with:');
  console.log('- Field assignments:', fieldAssignments);
  console.log('- Train percentage:', trainPercentage);
  console.log('- Selected fields:', selectedFields ? selectedFields.length : 'none');
  console.log('- Derived fields:', Object.keys(derivedFields || {}));
  
  // Field mappings from standard names to actual data fields
  const exposureField = fieldAssignments['Exposure'];
  const gucField = fieldAssignments['Ultimate Claims'];
  const countField = fieldAssignments['Claim Count'];
  const yearField = fieldAssignments['Year'];
  
  // Always include the standard fields
  const requiredFields = [exposureField, gucField, countField, yearField];
  
  // Initialize selectedFieldsToUse with default value if none provided
  const selectedFieldsToUse = selectedFields || [];
  
  // Combine required fields with selected fields, removing duplicates
  const fieldsToInclude = Array.from(new Set([...requiredFields, ...selectedFieldsToUse]));
  console.log('Fields to include in output CSV:', fieldsToInclude.join(', '));

  // Also add derived field names to ensure we generate them
  const derivedFieldNames = Object.keys(derivedFields || {});
  console.log('Derived fields to include:', derivedFieldNames.join(', '));

  // Random seed for consistent splitting
  const random = (rowIndex) => {
    const x = Math.sin(rowIndex) * 10000;
    return x - Math.floor(x);
  };

  // Function to determine if row passes filters
  const passesFilters = (row) => {
    // Year filter
    const year = Number(row[yearField]);
    if (selectedYears && Object.keys(selectedYears).length > 0 && !selectedYears[year]) return false;
    
    // Data filters
    const exposure = parseFloat(row[exposureField]) || 0;
    const guc = parseFloat(row[gucField]) || 0;
    const count = parseFloat(row[countField]) || 0;
    
    if (filters.exposuresGreaterThan && exposure <= minExposureValue) return false;
    if (filters.exposureLessThanOrEqualToOne && exposure > 1) return false;
    if (filters.ultimateClaimsNonNegative && guc < 0) return false;
    if (filters.claimCountNonNegative && count < 0) return false;
    
    return true;
  };

  if (fileType === '.csv') {
    // Create write streams
    const trainStream = fs.createWriteStream(trainOutputPath);
    const testStream = fs.createWriteStream(testOutputPath);
    
    // Track if headers have been written
    let headersWritten = false;
    let totalProcessed = 0;
    let totalFiltered = 0;
    let trainCount = 0;
    let testCount = 0;
    
    return new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          totalProcessed++;
          
          // Skip if doesn't pass filters
          if (!passesFilters(row)) return;
          
          totalFiltered++;
          
          // Apply transformations (derived fields + standardized field names)
          const transformedRow = applyFieldTransformations(row, fieldAssignments, derivedFields, true);
          
          // Filter to only include selected fields and derived fields
          const filteredRow = {};
          
          // Add fields based on fieldsToInclude
          Object.keys(transformedRow).forEach(field => {
            // For standard fields, always include them
            if (['count', 'guc', 'exposure', 'year'].includes(field)) {
              filteredRow[field] = transformedRow[field];
              return;
            }
            
            // Always include derived fields
            if (derivedFieldNames.includes(field)) {
              filteredRow[field] = transformedRow[field];
              return;
            }
            
            // Only include other fields if they're in the selectedFields list
            if (!selectedFieldsToUse.length || selectedFieldsToUse.includes(field)) {
              filteredRow[field] = transformedRow[field];
            }
          });
          
          // Write headers if not yet written
          if (!headersWritten) {
            const headers = Object.keys(filteredRow).join(',');
            trainStream.write(headers + '\n');
            testStream.write(headers + '\n');
            headersWritten = true;
            console.log('CSV Headers written:', headers);
          }
          
          // Determine if row goes to train or test using seeded random
          const randomValue = random(totalFiltered);
          const isTrainRow = randomValue < (trainPercentage / 100);
          
          // Convert row to CSV line
          const values = Object.values(filteredRow).map(v => 
            v === null || v === undefined ? '' : 
            typeof v === 'string' && v.includes(',') ? `"${v}"` : v
          ).join(',');
          
          // Write to appropriate file
          if (isTrainRow) {
            trainStream.write(values + '\n');
            trainCount++;
          } else {
            testStream.write(values + '\n');
            testCount++;
          }
          
          // Log progress periodically
          if (totalProcessed % 10000 === 0) {
            console.log(`Processed ${totalProcessed} rows, filtered ${totalFiltered}, train: ${trainCount}, test: ${testCount}`);
          }
        })
        .on('end', () => {
          trainStream.end();
          testStream.end();
          console.log(`Completed train/test split: ${totalProcessed} processed, ${totalFiltered} passed filters`);
          console.log(`Train set: ${trainCount} rows, Test set: ${testCount} rows`);
          resolve({
            trainCount,
            testCount,
            totalFiltered,
            totalProcessed
          });
        })
        .on('error', (err) => {
          console.error('Error in train/test split:', err);
          trainStream.end();
          testStream.end();
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files would be similar but using parquet reader/writer
    // This is a placeholder - if you're actually using Parquet files, you'd need to implement this part
    throw new Error('Parquet file handling not yet implemented for createTrainTestSplitFiles');
  }
  
  throw new Error('Unsupported file type for train/test split');
}

// Provide status for dataset
app.get('/api/dataset/:datasetId', (req, res) => {
  try {
    const { datasetId } = req.params;
    const configPath = path.join(__dirname, 'datasets', datasetId, 'config.json');
    
    if (!fs.existsSync(configPath)) {
      return res.status(404).json({ error: 'Dataset not found' });
    }
    
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    const trainPath = path.join(__dirname, 'datasets', datasetId, 'train.csv');
    const testPath = path.join(__dirname, 'datasets', datasetId, 'test.csv');
    
    const trainSize = fs.existsSync(trainPath) ? 
      fs.statSync(trainPath).size : 0;
      
    const testSize = fs.existsSync(testPath) ? 
      fs.statSync(testPath).size : 0;
    
    res.json({
      datasetId,
      config,
      trainSize,
      testSize,
      createdAt: new Date(parseInt(datasetId.split('_')[1])).toISOString()
    });
  } catch (error) {
    console.error('Error getting dataset status:', error);
    res.status(500).json({ error: 'Failed to get dataset status: ' + error.message });
  }
});

// Get distribution data for a field
// Updated /api/distribution endpoint to properly pass the binningMethod parameter

app.post('/api/distribution', express.json(), async (req, res) => {
  try {
    const { fileId, field, binningMethod, nonZeroOnly, isDerived } = req.body;
    console.log(`Distribution request for field: ${field}, fileId: ${fileId}, binningMethod: ${binningMethod}, isDerived: ${isDerived}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      console.log(`Session not found for fileId: ${fileId}`);
      return res.status(404).json({ error: 'File session not found' });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Add this block to check and apply session filters
    const applySessionFilters = session.filters && Object.values(session.filters).some(Boolean);
    const yearFilterActive = session.selectedYears && Object.keys(session.selectedYears).length > 0;
    const minExposureValue = session.minExposureValue || 0;
    
    // Logging filter state
    console.log('Filter state for distribution calculation:');
    console.log('- Applying data filters:', applySessionFilters);
    console.log('- Year filter active:', yearFilterActive);
    console.log('- Min exposure value:', minExposureValue);
    if (applySessionFilters) console.log('- Active filters:', session.filters);
    
    // Check if this is a standardized field name (count, guc, exposure, year)
    // If so, map it to the actual field name from the data
    let actualField = field;
    const standardFields = {
      'count': session.fieldAssignments ? session.fieldAssignments['Claim Count'] : null,
      'guc': session.fieldAssignments ? session.fieldAssignments['Ultimate Claims'] : null,
      'exposure': session.fieldAssignments ? session.fieldAssignments['Exposure'] : null,
      'year': session.fieldAssignments ? session.fieldAssignments['Year'] : null
    };
    
    if (standardFields[field]) {
      actualField = standardFields[field];
      console.log(`Mapping standardized field ${field} to actual field ${actualField}`);
    }
    
    // Check if this is a derived field and get its config
    const derivedFieldConfig = isDerived && session.derivedFields && session.derivedFields[field] ? 
      session.derivedFields[field] : null;
    
    console.log(`Field ${field} derived config:`, derivedFieldConfig);
    
    // Special handling for banded variables
    if (derivedFieldConfig && derivedFieldConfig.type === 'banded') {
      // ... existing banded variable code ...
    }
    
    // For regular fields or derived fields that aren't banded type
    console.log(`Using standard distribution calculation for ${field} with binningMethod: ${binningMethod}`);
    
    // Handle capped variables (which are numeric)
    let fieldToUse = actualField; // Use the mapped field name
    if (derivedFieldConfig && derivedFieldConfig.type === 'capped') {
      // For capped variables, we need to use the source field and apply capping in the processing
      fieldToUse = derivedFieldConfig.sourceField;
      console.log(`Capped variable detected: Using source field ${fieldToUse} with cap ${derivedFieldConfig.threshold}`);
    }
    
    // Process the distribution with the actual field name
    // Now passing session filters to the calculateDistribution function
    const distributionData = await calculateDistribution(
      filePath, 
      fileType, 
      fieldToUse, 
      binningMethod,
      nonZeroOnly,
      derivedFieldConfig,
      // Pass filter configuration when filters are active
      (applySessionFilters || yearFilterActive) ? {
        exposureField: session.fieldAssignments['Exposure'],
        gucField: session.fieldAssignments['Ultimate Claims'],
        countField: session.fieldAssignments['Claim Count'],
        yearField: session.fieldAssignments['Year'],
        filters: session.filters,
        minExposureValue: session.minExposureValue,
        selectedYears: session.selectedYears
      } : null
    );
    
    res.json(distributionData);
  } catch (error) {
    console.error('Error calculating distribution:', error);
    res.status(500).json({ error: 'Failed to calculate distribution: ' + error.message });
  }
});

// Get quantile data for a field
app.post('/api/quantiles', express.json(), async (req, res) => {
  try {
    const { fileId, field, nonZeroOnly, percentileRange, isDerived } = req.body;
    console.log(`Quantile request for field: ${field}, fileId: ${fileId}, nonZeroOnly: ${nonZeroOnly}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Check if this is a derived field (map standard fields to actual fields if needed)
    let fieldToUse = field;
    let derivedFieldConfig = null;
    
    // Standard field mapping
    if (['count', 'guc', 'exposure', 'year'].includes(field) && session.fieldAssignments) {
      const standardFields = {
        'count': session.fieldAssignments['Claim Count'],
        'guc': session.fieldAssignments['Ultimate Claims'],
        'exposure': session.fieldAssignments['Exposure'],
        'year': session.fieldAssignments['Year']
      };
      
      if (standardFields[field]) {
        fieldToUse = standardFields[field];
        console.log(`Mapped standard field ${field} to actual field ${fieldToUse}`);
      }
    }
    
    // Handle derived fields
    if (isDerived && session.derivedFields && session.derivedFields[field]) {
      derivedFieldConfig = session.derivedFields[field];
      console.log(`Using derived field config for ${field}:`, derivedFieldConfig);
      
      if (derivedFieldConfig.type === 'capped') {
        // For capped fields, we need to use the source field and apply the cap
        fieldToUse = derivedFieldConfig.sourceField;
        console.log(`Using source field ${fieldToUse} for capped variable ${field}`);
      }
    }
    
    // Calculate quantiles with the correct options
    const quantileData = await calculateQuantiles(
      filePath, 
      fileType, 
      fieldToUse,
      {
        nonZeroOnly: nonZeroOnly === true,
        percentileRange: percentileRange || {
          start: 90,
          end: 100, 
          step: 0.5
        },
        isDerived: !!derivedFieldConfig,
        derivedFieldConfig
      }
    );
    
    res.json(quantileData);
  } catch (error) {
    console.error('Error calculating quantiles:', error);
    res.status(500).json({ error: 'Failed to calculate quantiles: ' + error.message });
  }
});

// Get categorical distribution for a field
app.post('/api/categorical-distribution', express.json(), async (req, res) => {
  try {
    const { fileId, field } = req.body;
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Calculate categorical distribution
    const categoricalData = await calculateCategoricalDistribution(
      filePath, 
      fileType, 
      field
    );
    
    res.json({ categoricalData });
  } catch (error) {
    console.error('Error calculating categorical distribution:', error);
    res.status(500).json({ error: 'Failed to calculate categorical distribution: ' + error.message });
  }
});

// Get preview for banded variable
app.post('/api/band-preview', express.json(), async (req, res) => {
  try {
    const { fileId, field, bands } = req.body;
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Calculate band preview
    const preview = await calculateBandPreview(
      filePath, 
      fileType, 
      field,
      bands
    );
    
    res.json({ preview });
  } catch (error) {
    console.error('Error calculating band preview:', error);
    res.status(500).json({ error: 'Failed to calculate band preview: ' + error.message });
  }
});

// Create equal count bands
app.post('/api/equal-count-bands', express.json(), async (req, res) => {
  try {
    const { fileId, field, numBands } = req.body;
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Generate equal count bands
    const bands = await generateEqualCountBands(
      filePath, 
      fileType, 
      field,
      numBands || 4
    );
    
    res.json({ bands });
  } catch (error) {
    console.error('Error generating equal count bands:', error);
    res.status(500).json({ error: 'Failed to generate equal count bands: ' + error.message });
  }
});

// Create banded variable
app.post('/api/create-banded-variable', express.json(), async (req, res) => {
  try {
    const { fileId, field, bandedVariableName, bands } = req.body;
    
    console.log('Creating banded variable:', { fileId, field, bandedVariableName });
    console.log('Bands:', bands);
    
    // Validate required fields
    if (!fileId) return res.status(400).json({ error: 'Missing fileId' });
    if (!field) return res.status(400).json({ error: 'Missing source field' });
    if (!bandedVariableName) return res.status(400).json({ error: 'Missing banded variable name' });
    if (!bands || !Array.isArray(bands) || bands.length === 0) {
      return res.status(400).json({ error: 'Invalid or empty bands array' });
    }
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Validate each band to make sure it has required properties
    for (const band of bands) {
      if (!band.name || band.min === undefined || band.max === undefined) {
        return res.status(400).json({ error: 'Each band must have a name, min, and max value' });
      }
      
      // Make sure min and max are valid numbers
      if (isNaN(Number(band.min)) || isNaN(Number(band.max))) {
        return res.status(400).json({ error: 'Min and max values must be valid numbers' });
      }
    }
    
    // Initialize derivedFields object if it doesn't exist
    if (!session.derivedFields) {
      session.derivedFields = {};
    }
    
    // Store the banding configuration with the session
    session.derivedFields[bandedVariableName] = {
      type: 'banded',
      sourceField: field,
      bands: bands
    };
    
    console.log('Banded variable created:', bandedVariableName);
    
    // Return success response
    res.json({ 
      success: true,
      bandedVariableName,
      message: `Banded variable '${bandedVariableName}' created successfully`
    });
  } catch (error) {
    console.error('Error creating banded variable:', error);
    res.status(500).json({ error: 'Failed to create banded variable: ' + error.message });
  }
});

// Add a debug endpoint to check derived fields
app.get('/api/derived-fields/:fileId', (req, res) => {
  try {
    const { fileId } = req.params;
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    res.json({
      derivedFields: session.derivedFields || {}
    });
  } catch (error) {
    console.error('Error retrieving derived fields:', error);
    res.status(500).json({ error: 'Failed to retrieve derived fields: ' + error.message });
  }
});

// Create capped variable
app.post('/api/create-capped-variable', express.json(), async (req, res) => {
  try {
    const { fileId, field, cappedField, threshold } = req.body;
    
    // Validate required fields
    if (!fileId) return res.status(400).json({ error: 'Missing fileId' });
    if (!field) return res.status(400).json({ error: 'Missing source field' });
    if (!cappedField) return res.status(400).json({ error: 'Missing capped field name' });
    if (threshold === undefined || threshold === null) {
      return res.status(400).json({ error: 'Missing threshold value' });
    }
    
    console.log(`Creating capped variable ${cappedField} from ${field} with threshold ${threshold}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Make sure threshold is a valid number
    const parsedThreshold = Number(threshold);
    if (isNaN(parsedThreshold)) {
      return res.status(400).json({ error: 'Threshold must be a valid number' });
    }
    
    // Check if field is a standard field name and get the actual field name if needed
    let actualSourceField = field;
    
    // If it's a standard field name, map it to the actual field name
    if (['count', 'guc', 'exposure', 'year'].includes(field) && session.fieldAssignments) {
      const standardMapping = {
        'count': session.fieldAssignments['Claim Count'],
        'guc': session.fieldAssignments['Ultimate Claims'],
        'exposure': session.fieldAssignments['Exposure'],
        'year': session.fieldAssignments['Year']
      };
      
      if (standardMapping[field]) {
        actualSourceField = standardMapping[field];
        console.log(`Mapped standard field '${field}' to actual field '${actualSourceField}'`);
      } else {
        console.warn(`Standard field '${field}' has no mapping in the dataset`);
      }
    }
    
    // Check if actual source field exists
    let fieldExists = false;
    if (session.metadata && session.metadata.stats) {
      fieldExists = Object.keys(session.metadata.stats).includes(actualSourceField);
    }
    
    if (!fieldExists) {
      console.warn(`Field '${actualSourceField}' not found in metadata stats`);
      
      // If it's a derived field, check if it exists in derivedFields
      if (session.derivedFields && session.derivedFields[actualSourceField]) {
        console.log(`Field '${actualSourceField}' found in derived fields`);
        fieldExists = true;
      } else {
        return res.status(400).json({ error: `Field '${actualSourceField}' does not exist in the dataset` });
      }
    }
    
    // Initialize derivedFields object if it doesn't exist
    if (!session.derivedFields) {
      session.derivedFields = {};
    }
    
    // Check if the capped field already exists - but we'll allow overwriting it
    const isOverwriting = session.derivedFields[cappedField] !== undefined;
    
    // Store or update the capping configuration
    session.derivedFields[cappedField] = {
      type: 'capped',
      sourceField: actualSourceField,
      threshold: parsedThreshold
    };
    
    const actionType = isOverwriting ? 'Updated' : 'Created';
    console.log(`${actionType} capped variable: ${cappedField} from ${actualSourceField} with threshold ${parsedThreshold}`);
    console.log('Updated session.derivedFields:', session.derivedFields);
    
    // Return success response with details
    res.json({
      success: true,
      cappedField,
      sourceField: actualSourceField,
      threshold: parsedThreshold,
      isOverwriting: isOverwriting
    });
  } catch (error) {
    console.error('Error creating capped variable:', error);
    res.status(500).json({ error: 'Failed to create capped variable: ' + error.message });
  }
});

// Visualization functions

// Complete updated calculateDistribution function
async function calculateDistribution(
  filePath, 
  fileType, 
  field, 
  binningMethod, 
  nonZeroOnly = false, 
  derivedFieldConfig = null,
  filterConfig = null  // New parameter for filter configuration
) {
  console.log(`Calculating distribution for ${field} using binning method: ${binningMethod || 'reed'}`);
  if (filterConfig) {
    console.log('Applying filters to distribution calculation');
  }
  
  // Function to process values based on derivedFieldConfig
  const processValue = (value, derivedFieldConfig) => {
    if (!derivedFieldConfig) return value;
    
    // For capped fields, apply the cap
    if (derivedFieldConfig.type === 'capped') {
      const numValue = parseFloat(value);
      if (isNaN(numValue)) return value;
      return Math.min(numValue, derivedFieldConfig.threshold);
    }
    
    return value;
  };
  
  // Helper function to safely parse numeric values
  const safeParseFloat = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  };
  
  // New helper function to check if a row passes filters
  const passesFilters = (row, filterConfig) => {
    if (!filterConfig) return true; // No filters to apply
    
    const { exposureField, gucField, countField, yearField, filters, minExposureValue, selectedYears } = filterConfig;
    
    // Year filter
    if (selectedYears && yearField) {
      const year = Number(row[yearField]);
      if (!selectedYears[year]) return false;
    }
    
    // Data filters
    if (filters) {
      const exposure = safeParseFloat(row[exposureField]) || 0;
      const guc = safeParseFloat(row[gucField]) || 0;
      const count = safeParseFloat(row[countField]) || 0;
      
      if (filters.exposuresGreaterThan && exposure <= minExposureValue) return false;
      if (filters.exposureLessThanOrEqualToOne && exposure > 1) return false;
      if (filters.ultimateClaimsNonNegative && guc < 0) return false;
      if (filters.claimCountNonNegative && count < 0) return false;
    }
    
    return true;
  };
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Arrays to collect data for statistical calculations
      const values = [];
      let rowCount = 0;
      let validCount = 0;
      let invalidCount = 0;
      let filteredOutCount = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          rowCount++;
          
          // Apply session filters first, if available
          if (filterConfig && !passesFilters(row, filterConfig)) {
            filteredOutCount++;
            return; // Skip this row as it doesn't pass filters
          }
          
          // Check if field exists in the row
          if (!(field in row)) {
            if (rowCount <= 5) {
              console.log(`Field ${field} not found in row ${rowCount}`);
            }
            invalidCount++;
            return;
          }
          
          let value = row[field];
          
          if (value === null || value === undefined || value === '') {
            invalidCount++;
            return;
          }
          
          // Apply derivedFieldConfig if present
          if (derivedFieldConfig) {
            value = processValue(value, derivedFieldConfig);
          }
          
          // Parse the value
          let parsedValue = safeParseFloat(value);
          if (parsedValue === null) {
            invalidCount++;
            return;
          }
          
          // Skip zero values if nonZeroOnly is true
          if (nonZeroOnly && parsedValue <= 0) {
            invalidCount++;
            return;
          }
          
          validCount++;
          values.push(parsedValue);
          
          // Log progress for large files
          if (rowCount % 100000 === 0) {
            console.log(`Processed ${rowCount} rows, found ${validCount} valid values for ${field}, filtered out ${filteredOutCount} rows by session filters`);
          }
        })
        .on('end', () => {
          console.log(`Completed distribution calculation: ${rowCount} rows, ${validCount} valid values, ${invalidCount} invalid for ${field}, ${filteredOutCount} filtered out by session filters`);
          
          if (values.length === 0) {
            console.log(`No valid values found for ${field}`);
            resolve({ 
              histogramData: null, 
              boxPlotData: null, 
              stats: null, 
              isCategorical: false,
              filteredRows: rowCount - filteredOutCount,
              totalRows: rowCount,
              filterApplied: filteredOutCount > 0
            });
            return;
          }
          
          // Sort values for quantile calculations
          values.sort((a, b) => a - b);
          
          // Calculate statistics
          const sum = values.reduce((acc, val) => acc + val, 0);
          const mean = sum / values.length;
          const min = values[0];
          const max = values[values.length - 1];
          
          // Count positive values
          const positiveCount = values.filter(val => val > 0).length;
          
          const stats = { 
            mean, 
            min, 
            max, 
            positiveCount,
            positivePercentage: ((positiveCount / values.length) * 100).toFixed(2)
          };
          
          console.log(`Stats for ${field}: min=${min}, max=${max}, mean=${mean}, positive values: ${positiveCount}/${values.length} (${stats.positivePercentage}%)`);
          
          // Calculate bins based on binning method
          let binCount;
          // Ensure binningMethod has a default and is handled properly
          const method = binningMethod || 'reed';
          
          if (method === 'doane') {
            // Doane's formula
            console.log(`Using Doane's formula for binning`);
            const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
            const stdDev = Math.sqrt(variance);
            
            // Handle potential division by zero
            let skewness = 0;
            if (stdDev > 0) {
              skewness = values.reduce((acc, val) => acc + Math.pow((val - mean) / stdDev, 3), 0) / values.length;
            }
            
            // Calculate bin count with protection against invalid inputs
            const divisor = Math.sqrt(6 / Math.max(values.length - 2, 1));
            binCount = Math.ceil(1 + Math.log2(values.length) + Math.log2(1 + Math.abs(skewness) / divisor));
          } else {
            // Reed's formula (default)
            console.log(`Using Reed's formula for binning`);
            binCount = Math.ceil(1 + Math.log2(values.length));
          }
          
          // Adjust bin count to reasonable limits
          binCount = Math.min(Math.max(binCount, 5), 50);
          console.log(`Using ${binCount} bins for histogram with method ${method}`);
          
          // Create bins
          const binSize = (max - min) / binCount;
          const bins = new Array(binCount).fill(0);
          
          // Count values in each bin
          values.forEach(value => {
            const binIndex = Math.min(Math.floor((value - min) / binSize), binCount - 1);
            if (binIndex >= 0 && binIndex < bins.length) {
              bins[binIndex]++;
            }
          });
          
          // Format histogram data
          const histogramData = bins.map((count, i) => {
            const start = min + (i * binSize);
            const end = start + binSize;
            return {
              range: `${start.toFixed(2)} - ${end.toFixed(2)}`,
              count
            };
          });
          
          // Calculate box plot data
          const calculateQuantile = (arr, q) => {
            const pos = (arr.length - 1) * q;
            const base = Math.floor(pos);
            const rest = pos - base;
            if (arr[base + 1] !== undefined) {
              return arr[base] + rest * (arr[base + 1] - arr[base]);
            }
            return arr[base];
          };
          
          const boxPlotData = {
            min,
            q1: calculateQuantile(values, 0.25),
            median: calculateQuantile(values, 0.5),
            q3: calculateQuantile(values, 0.75),
            max
          };
          
          // Updated response to include filter information
          const result = { 
            histogramData, 
            boxPlotData, 
            stats,
            isCategorical: false,
            filteredRows: rowCount - filteredOutCount, // Add count of rows after filtering
            totalRows: rowCount,  // Total row count before filtering
            filterApplied: filteredOutCount > 0 // Flag indicating if filters were applied
          };
          
          resolve(result);
        })
        .on('error', (err) => {
          console.error(`Error processing CSV for ${field}:`, err);
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files with filter support
    try {
      console.log('Processing Parquet file for distribution calculation');
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      // Arrays to collect data for statistical calculations
      const values = [];
      let rowCount = 0;
      let validCount = 0;
      let invalidCount = 0;
      let filteredOutCount = 0;
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        rowCount++;
        
        // Apply session filters first, if available
        if (filterConfig && !passesFilters(row, filterConfig)) {
          filteredOutCount++;
          continue; // Skip this row as it doesn't pass filters
        }
        
        // Check if field exists in the row
        if (!(field in row)) {
          if (rowCount <= 5) {
            console.log(`Field ${field} not found in row ${rowCount}`);
          }
          invalidCount++;
          continue;
        }
        
        let value = row[field];
        
        if (value === null || value === undefined || value === '') {
          invalidCount++;
          continue;
        }
        
        // Apply derivedFieldConfig if present
        if (derivedFieldConfig) {
          value = processValue(value, derivedFieldConfig);
        }
        
        // Parse the value
        let parsedValue = safeParseFloat(value);
        if (parsedValue === null) {
          invalidCount++;
          continue;
        }
        
        // Skip zero values if nonZeroOnly is true
        if (nonZeroOnly && parsedValue <= 0) {
          invalidCount++;
          continue;
        }
        
        validCount++;
        values.push(parsedValue);
        
        // Log progress periodically
        if (rowCount % 100000 === 0) {
          console.log(`Processed ${rowCount} Parquet rows, found ${validCount} valid values for ${field}, filtered out ${filteredOutCount} rows by session filters`);
        }
      }
      
      await reader.close();
      
      console.log(`Completed Parquet distribution calculation: ${rowCount} rows, ${validCount} valid values, ${invalidCount} invalid for ${field}, ${filteredOutCount} filtered out by session filters`);
      
      if (values.length === 0) {
        console.log(`No valid values found for ${field} in Parquet file`);
        return { 
          histogramData: null, 
          boxPlotData: null, 
          stats: null, 
          isCategorical: false,
          filteredRows: rowCount - filteredOutCount,
          totalRows: rowCount,
          filterApplied: filteredOutCount > 0
        };
      }
      
      // Sort values for quantile calculations
      values.sort((a, b) => a - b);
      
      // Calculate statistics
      const sum = values.reduce((acc, val) => acc + val, 0);
      const mean = sum / values.length;
      const min = values[0];
      const max = values[values.length - 1];
      
      // Count positive values
      const positiveCount = values.filter(val => val > 0).length;
      
      const stats = { 
        mean, 
        min, 
        max, 
        positiveCount,
        positivePercentage: ((positiveCount / values.length) * 100).toFixed(2)
      };
      
      console.log(`Stats for ${field} from Parquet: min=${min}, max=${max}, mean=${mean}, positive values: ${positiveCount}/${values.length} (${stats.positivePercentage}%)`);
      
      // Calculate bins based on binning method
      let binCount;
      // Ensure binningMethod has a default and is handled properly
      const method = binningMethod || 'reed';
      
      if (method === 'doane') {
        // Doane's formula
        console.log(`Using Doane's formula for binning`);
        const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
        const stdDev = Math.sqrt(variance);
        
        // Handle potential division by zero
        let skewness = 0;
        if (stdDev > 0) {
          skewness = values.reduce((acc, val) => acc + Math.pow((val - mean) / stdDev, 3), 0) / values.length;
        }
        
        // Calculate bin count with protection against invalid inputs
        const divisor = Math.sqrt(6 / Math.max(values.length - 2, 1));
        binCount = Math.ceil(1 + Math.log2(values.length) + Math.log2(1 + Math.abs(skewness) / divisor));
      } else {
        // Reed's formula (default)
        console.log(`Using Reed's formula for binning`);
        binCount = Math.ceil(1 + Math.log2(values.length));
      }
      
      // Adjust bin count to reasonable limits
      binCount = Math.min(Math.max(binCount, 5), 50);
      console.log(`Using ${binCount} bins for histogram with method ${method}`);
      
      // Create bins
      const binSize = (max - min) / binCount;
      const bins = new Array(binCount).fill(0);
      
      // Count values in each bin
      values.forEach(value => {
        const binIndex = Math.min(Math.floor((value - min) / binSize), binCount - 1);
        if (binIndex >= 0 && binIndex < bins.length) {
          bins[binIndex]++;
        }
      });
      
      // Format histogram data
      const histogramData = bins.map((count, i) => {
        const start = min + (i * binSize);
        const end = start + binSize;
        return {
          range: `${start.toFixed(2)} - ${end.toFixed(2)}`,
          count
        };
      });
      
      // Calculate box plot data
      const calculateQuantile = (arr, q) => {
        const pos = (arr.length - 1) * q;
        const base = Math.floor(pos);
        const rest = pos - base;
        if (arr[base + 1] !== undefined) {
          return arr[base] + rest * (arr[base + 1] - arr[base]);
        }
        return arr[base];
      };
      
      const boxPlotData = {
        min,
        q1: calculateQuantile(values, 0.25),
        median: calculateQuantile(values, 0.5),
        q3: calculateQuantile(values, 0.75),
        max
      };
      
      // Return result with filter information
      return { 
        histogramData, 
        boxPlotData, 
        stats,
        isCategorical: false,
        filteredRows: rowCount - filteredOutCount,
        totalRows: rowCount,
        filterApplied: filteredOutCount > 0
      };
    } catch (error) {
      console.error('Error calculating distribution for Parquet file:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

const passesFilters = (row, filterConfig) => {
  if (!filterConfig) return true; // No filters to apply
  
  const { exposureField, gucField, countField, yearField, filters, minExposureValue, selectedYears } = filterConfig;
  
  // Year filter
  if (selectedYears && yearField) {
    const year = Number(row[yearField]);
    if (!selectedYears[year]) return false;
  }
  
  // Data filters
  if (filters) {
    const exposure = safeParseFloat(row[exposureField]) || 0;
    const guc = safeParseFloat(row[gucField]) || 0;
    const count = safeParseFloat(row[countField]) || 0;
    
    if (filters.exposuresGreaterThan && exposure <= minExposureValue) return false;
    if (filters.exposureLessThanOrEqualToOne && exposure > 1) return false;
    if (filters.ultimateClaimsNonNegative && guc < 0) return false;
    if (filters.claimCountNonNegative && count < 0) return false;
  }
  
  return true;
};

// Complete updated calculateQuantiles function for server.js

async function calculateQuantiles(filePath, fileType, field, options = {}) {
  // Extract options with defaults
  const {
    nonZeroOnly = false,
    percentileRange = {
      start: 90,
      end: 100,
      step: 0.5,
      additionalRanges: []
    },
    isDerived = false,
    derivedFieldConfig = null
  } = options;
  
  console.log(`Calculating quantiles for ${field}, nonZeroOnly: ${nonZeroOnly}, derived: ${isDerived}`);
  if (percentileRange.additionalRanges && percentileRange.additionalRanges.length > 0) {
    console.log('With additional percentile ranges:', percentileRange.additionalRanges);
  }

  // Helper function to safely parse numeric values
  const safeParseFloat = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  };
  
  // Function to process values based on derivedFieldConfig
  const processValue = (value, derivedFieldConfig) => {
    if (!derivedFieldConfig) return value;
    
    // For capped fields, apply the cap
    if (derivedFieldConfig.type === 'capped') {
      const numValue = safeParseFloat(value);
      if (numValue === null) return value;
      return Math.min(numValue, derivedFieldConfig.threshold);
    }
    
    return value;
  };
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Collect non-zero values
      const values = [];
      let processedRows = 0;
      let skippedRows = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          delimiter: [',', '\t', '|', ';'] // Try multiple delimiters
        }))
        .on('data', (row) => {
          processedRows++;
          
          let value;
          
          // If this is a derived field, we might need special handling
          if (derivedFieldConfig) {
            if (derivedFieldConfig.type === 'capped') {
              // For capped variables, we need to get the source field and apply capping
              const sourceField = derivedFieldConfig.sourceField;
              value = row[sourceField]; 
              value = processValue(value, derivedFieldConfig);
            } else {
              // Skip other derived field types for now
              skippedRows++;
              return;
            }
          } else {
            // Standard field
            value = row[field];
          }
          
          if (value === null || value === undefined || value === '') {
            skippedRows++;
            return;
          }
          
          // Parse the value
          let parsedValue;
          if (typeof value === 'number') {
            parsedValue = value;
          } else if (typeof value === 'string') {
            parsedValue = safeParseFloat(value.replace(/,/g, ''));
          } else {
            skippedRows++;
            return;
          }
          
          // Skip if not a valid number
          if (parsedValue === null || !isFinite(parsedValue)) {
            skippedRows++;
            return;
          }
          
          // Skip zero/negative values if nonZeroOnly is true
          if (nonZeroOnly && parsedValue <= 0) {
            skippedRows++;
            return;
          }
          
          values.push(parsedValue);
          
          // Log progress for large files
          if (processedRows % 100000 === 0) {
            console.log(`Processed ${processedRows.toLocaleString()} rows calculating quantiles for ${field}, collected ${values.length.toLocaleString()} valid values`);
          }
        })
        .on('end', () => {
          console.log(`Finished processing ${processedRows.toLocaleString()} rows, found ${values.length.toLocaleString()} valid values for quantile calculations (skipped ${skippedRows.toLocaleString()} rows)`);
          
          if (values.length === 0) {
            console.log(`No valid values found for ${field} quantile calculations`);
            resolve({ 
              quantiles: {},
              valueCount: 0,
              nonZeroOnly 
            });
            return;
          }
          
          // Sort values for quantile calculations
          values.sort((a, b) => a - b);
          
          // Calculate quantiles based on percentileRange
          const quantiles = {};
          const start = percentileRange.start || 90;
          const end = percentileRange.end || 100;
          const step = percentileRange.step || 0.5;
          
          // Function to calculate a specific quantile
          const calculateQuantile = (arr, q) => {
            if (arr.length === 0) return null;
            
            if (q === 0) return arr[0];
            if (q === 100) return arr[arr.length - 1];
            
            const pos = (arr.length - 1) * (q / 100);
            const base = Math.floor(pos);
            const rest = pos - base;
            
            if (base + 1 < arr.length) {
              return arr[base] + rest * (arr[base + 1] - arr[base]);
            }
            return arr[base];
          };
          
          // Calculate all requested percentiles in the main range
          for (let i = start; i <= end; i += step) {
            const percentileKey = i.toFixed(1);
            quantiles[percentileKey] = calculateQuantile(values, i);
          }
          
          // Calculate additional ranges with different granularity if specified
          if (percentileRange.additionalRanges && Array.isArray(percentileRange.additionalRanges)) {
            percentileRange.additionalRanges.forEach(range => {
              const rangeStart = range.start || 99.5;
              const rangeEnd = range.end || 99.9;
              const rangeStep = range.step || 0.1;
              
              console.log(`Calculating additional percentile range: ${rangeStart}-${rangeEnd} with step ${rangeStep}`);
              
              for (let i = rangeStart; i <= rangeEnd; i += rangeStep) {
                const percentileKey = i.toFixed(1);
                if (!quantiles[percentileKey]) { // Avoid duplicate calculations
                  quantiles[percentileKey] = calculateQuantile(values, i);
                }
              }
            });
          }
          
          // Always add key statistics
          quantiles['min'] = values[0];
          quantiles['max'] = values[values.length - 1];
          quantiles['median'] = calculateQuantile(values, 50);
          quantiles['mean'] = values.reduce((sum, val) => sum + val, 0) / values.length;
          
          // Calculate standard deviation
          const mean = quantiles['mean'];
          const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
          quantiles['std_dev'] = Math.sqrt(variance);
          
          console.log(`Calculated ${Object.keys(quantiles).length} quantiles for ${field}`);
          
          resolve({ 
            quantiles,
            valueCount: values.length,
            nonZeroOnly,
            totalRows: processedRows
          });
        })
        .on('error', (err) => {
          console.error(`Error calculating quantiles for ${field}:`, err);
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files
    try {
      console.log(`Opening parquet file for quantile calculations for ${field}: ${filePath}`);
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      const values = [];
      let processedRows = 0;
      let skippedRows = 0;
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        processedRows++;
        
        let value;
        
        // If this is a derived field, we might need special handling
        if (derivedFieldConfig) {
          if (derivedFieldConfig.type === 'capped') {
            // For capped variables, we need to get the source field and apply capping
            const sourceField = derivedFieldConfig.sourceField;
            value = row[sourceField]; 
            value = processValue(value, derivedFieldConfig);
          } else {
            // Skip other derived field types for now
            skippedRows++;
            continue;
          }
        } else {
          // Standard field
          value = row[field];
        }
        
        if (value === null || value === undefined || value === '') {
          skippedRows++;
          continue;
        }
        
        // Parse the value
        let parsedValue;
        if (typeof value === 'number') {
          parsedValue = value;
        } else if (typeof value === 'string') {
          parsedValue = safeParseFloat(value.replace(/,/g, ''));
        } else {
          skippedRows++;
          continue;
        }
        
        // Skip if not a valid number
        if (parsedValue === null || !isFinite(parsedValue)) {
          skippedRows++;
          continue;
        }
        
        // Skip zero/negative values if nonZeroOnly is true
        if (nonZeroOnly && parsedValue <= 0) {
          skippedRows++;
          continue;
        }
        
        values.push(parsedValue);
        
        // Log progress for large files
        if (processedRows % 100000 === 0) {
          console.log(`Processed ${processedRows.toLocaleString()} parquet rows calculating quantiles for ${field}`);
        }
      }
      
      await reader.close();
      
      console.log(`Finished processing ${processedRows.toLocaleString()} parquet rows, found ${values.length.toLocaleString()} valid values for quantile calculations (skipped ${skippedRows.toLocaleString()} rows)`);
      
      if (values.length === 0) {
        console.log(`No valid values found for ${field} quantile calculations in parquet file`);
        return { 
          quantiles: {},
          valueCount: 0,
          nonZeroOnly 
        };
      }
      
      // Sort values for quantile calculations
      values.sort((a, b) => a - b);
      
      // Calculate quantiles based on percentileRange
      const quantiles = {};
      const start = percentileRange.start || 90;
      const end = percentileRange.end || 100;
      const step = percentileRange.step || 0.5;
      
      // Function to calculate a specific quantile
      const calculateQuantile = (arr, q) => {
        if (arr.length === 0) return null;
        
        if (q === 0) return arr[0];
        if (q === 100) return arr[arr.length - 1];
        
        const pos = (arr.length - 1) * (q / 100);
        const base = Math.floor(pos);
        const rest = pos - base;
        
        if (base + 1 < arr.length) {
          return arr[base] + rest * (arr[base + 1] - arr[base]);
        }
        return arr[base];
      };
      
      // Calculate all requested percentiles in the main range
      for (let i = start; i <= end; i += step) {
        const percentileKey = i.toFixed(1);
        quantiles[percentileKey] = calculateQuantile(values, i);
      }
      
      // Calculate additional ranges with different granularity if specified
      if (percentileRange.additionalRanges && Array.isArray(percentileRange.additionalRanges)) {
        percentileRange.additionalRanges.forEach(range => {
          const rangeStart = range.start || 99.5;
          const rangeEnd = range.end || 99.9;
          const rangeStep = range.step || 0.1;
          
          console.log(`Calculating additional percentile range: ${rangeStart}-${rangeEnd} with step ${rangeStep}`);
          
          for (let i = rangeStart; i <= rangeEnd; i += rangeStep) {
            const percentileKey = i.toFixed(1);
            if (!quantiles[percentileKey]) { // Avoid duplicate calculations
              quantiles[percentileKey] = calculateQuantile(values, i);
            }
          }
        });
      }
      
      // Always add key statistics
      quantiles['min'] = values[0];
      quantiles['max'] = values[values.length - 1];
      quantiles['median'] = calculateQuantile(values, 50);
      quantiles['mean'] = values.reduce((sum, val) => sum + val, 0) / values.length;
      
      // Calculate standard deviation
      const mean = quantiles['mean'];
      const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
      quantiles['std_dev'] = Math.sqrt(variance);
      
      console.log(`Calculated ${Object.keys(quantiles).length} quantiles for ${field} from parquet`);
      
      return { 
        quantiles,
        valueCount: values.length,
        nonZeroOnly,
        totalRows: processedRows
      };
    } catch (error) {
      console.error(`Error calculating quantiles for ${field} from parquet:`, error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type for quantile calculations');
}

async function calculateCategoricalDistribution(filePath, fileType, field) {
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Count occurrences of each category
      const valueCounts = {};
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          const value = row[field];
          
          if (value === null || value === undefined) {
            return;
          }
          
          const strValue = String(value).trim();
          if (strValue === '') {
            return;
          }
          
          valueCounts[strValue] = (valueCounts[strValue] || 0) + 1;
        })
        .on('end', () => {
          // Format the data for the chart
          const categoricalData = Object.entries(valueCounts)
            .map(([category, count]) => ({
              category,
              count
            }))
            .sort((a, b) => {
              const numA = parseFloat(a.category);
              const numB = parseFloat(b.category);
              
              if (!isNaN(numA) && !isNaN(numB)) {
                return numA - numB;
              }
              
              return b.count - a.count;
            });
          
          resolve(categoricalData);
        })
        .on('error', reject);
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files
    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      const valueCounts = {};
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        const value = row[field];
        
        if (value === null || value === undefined) {
          continue;
        }
        
        const strValue = String(value).trim();
        if (strValue === '') {
          continue;
        }
        
        valueCounts[strValue] = (valueCounts[strValue] || 0) + 1;
      }
      
      await reader.close();
      
      // Format the data for the chart
      const categoricalData = Object.entries(valueCounts)
        .map(([category, count]) => ({
          category,
          count
        }))
        .sort((a, b) => {
          const numA = parseFloat(a.category);
          const numB = parseFloat(b.category);
          
          if (!isNaN(numA) && !isNaN(numB)) {
            return numA - numB;
          }
          
          return b.count - a.count;
        });
      
      return categoricalData;
    } catch (error) {
      console.error('Error calculating categorical distribution for Parquet:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

async function calculateBandPreview(filePath, fileType, field, bands) {
  // Sort bands by min value to ensure consistent handling of overlaps
  const sortedBands = [...bands].sort((a, b) => Number(a.min) - Number(b.min));
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Count records in each band
      const bandCounts = {};
      let totalCount = 0;
      
      // Initialize counts for each band
      bands.forEach(band => {
        bandCounts[band.name] = 0;
      });
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          const value = row[field];
          
          if (value === null || value === undefined || value === '') {
            return;
          }
          
          // Parse the value
          let parsedValue;
          if (typeof value === 'number') {
            parsedValue = value;
          } else if (typeof value === 'string') {
            parsedValue = parseFloat(value.replace(/,/g, ''));
          } else {
            return;
          }
          
          // Skip if not a valid number
          if (isNaN(parsedValue) || !isFinite(parsedValue)) {
            return;
          }
          
          totalCount++;
          
          // Assign to first matching band (important for handling overlaps)
          let assigned = false;
          for (const band of bands) {
            const min = Number(band.min);
            const max = Number(band.max);
            
            if (parsedValue >= min && parsedValue <= max) {
              bandCounts[band.name] = (bandCounts[band.name] || 0) + 1;
              assigned = true;
              break; // Stop at first matching band
            }
          }
          
          // Count unassigned values for reporting
          if (!assigned) {
            bandCounts['Unassigned'] = (bandCounts['Unassigned'] || 0) + 1;
          }
        })
        .on('end', () => {
          // Format the preview data
          const preview = Object.entries(bandCounts).map(([name, count]) => ({
            name,
            count,
            percentage: totalCount > 0 ? ((count / totalCount) * 100).toFixed(1) : "0.0"
          }));
          
          resolve(preview);
        })
        .on('error', reject);
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files would be similar but using the parquet reader
    // This is the same logic but with the Parquet API
    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      // Count records in each band
      const bandCounts = {};
      let totalCount = 0;
      
      // Initialize counts for each band
      bands.forEach(band => {
        bandCounts[band.name] = 0;
      });
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        const value = row[field];
        
        if (value === null || value === undefined || value === '') {
          continue;
        }
        
        // Parse the value
        let parsedValue;
        if (typeof value === 'number') {
          parsedValue = value;
        } else if (typeof value === 'string') {
          parsedValue = parseFloat(value.replace(/,/g, ''));
        } else {
          continue;
        }
        
        // Skip if not a valid number
        if (isNaN(parsedValue) || !isFinite(parsedValue)) {
          continue;
        }
        
        totalCount++;
        
        // Assign to first matching band (important for handling overlaps)
        let assigned = false;
        for (const band of bands) {
          const min = Number(band.min);
          const max = Number(band.max);
          
          if (parsedValue >= min && parsedValue <= max) {
            bandCounts[band.name] = (bandCounts[band.name] || 0) + 1;
            assigned = true;
            break; // Stop at first matching band
          }
        }
        
        // Count unassigned values for reporting
        if (!assigned) {
          bandCounts['Unassigned'] = (bandCounts['Unassigned'] || 0) + 1;
        }
      }
      
      await reader.close();
      
      // Format the preview data
      const preview = Object.entries(bandCounts).map(([name, count]) => ({
        name,
        count,
        percentage: totalCount > 0 ? ((count / totalCount) * 100).toFixed(1) : "0.0"
      }));
      
      return preview;
    } catch (error) {
      console.error('Error calculating band preview for Parquet:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

async function generateEqualCountBands(filePath, fileType, field, numBands) {
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Collect values for the field
      const values = [];
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          const value = row[field];
          
          if (value === null || value === undefined || value === '') {
            return;
          }
          
          // Parse the value
          let parsedValue;
          if (typeof value === 'number') {
            parsedValue = value;
          } else if (typeof value === 'string') {
            parsedValue = parseFloat(value.replace(/,/g, ''));
          } else {
            return;
          }
          
          // Skip if not a valid number
          if (isNaN(parsedValue) || !isFinite(parsedValue)) {
            return;
          }
          
          values.push(parsedValue);
        })
        .on('end', () => {
          if (values.length === 0) {
            resolve([]);
            return;
          }
          
          // Sort values
          values.sort((a, b) => a - b);
          
          // Create equal count bands
          const bands = [];
          
          for (let i = 0; i < numBands; i++) {
            const lowerPercentile = i / numBands;
            const upperPercentile = (i + 1) / numBands;
            
            const lowerIndex = Math.floor(values.length * lowerPercentile);
            const upperIndex = Math.min(
              Math.floor(values.length * upperPercentile),
              values.length - 1
            );
            
            const min = i === 0 ? 
              values[0] : 
              values[lowerIndex];
            
            const max = i === numBands - 1 ?
              values[values.length - 1] :
              values[upperIndex];
            
            bands.push({
              name: `Q${i + 1}`,
              min,
              max
            });
          }
          
          resolve(bands);
        })
        .on('error', reject);
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files
    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      // Collect values for the field
      const values = [];
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        const value = row[field];
        
        if (value === null || value === undefined || value === '') {
          continue;
        }
        
        // Parse the value
        let parsedValue;
        if (typeof value === 'number') {
          parsedValue = value;
        } else if (typeof value === 'string') {
          parsedValue = parseFloat(value.replace(/,/g, ''));
        } else {
          continue;
        }
        
        // Skip if not a valid number
        if (isNaN(parsedValue) || !isFinite(parsedValue)) {
          continue;
        }
        
        values.push(parsedValue);
      }
      
      await reader.close();
      
      if (values.length === 0) {
        return [];
      }
      
      // Sort values
      values.sort((a, b) => a - b);
      
      // Create equal count bands
      const bands = [];
      
      for (let i = 0; i < numBands; i++) {
        const lowerPercentile = i / numBands;
        const upperPercentile = (i + 1) / numBands;
        
        const lowerIndex = Math.floor(values.length * lowerPercentile);
        const upperIndex = Math.min(
          Math.floor(values.length * upperPercentile),
          values.length - 1
        );
        
        const min = i === 0 ? 
          values[0] : 
          values[lowerIndex];
        
        const max = i === numBands - 1 ?
          values[values.length - 1] :
          values[upperIndex];
        
        bands.push({
          name: `Q${i + 1}`,
          min,
          max
        });
      }
      
      return bands;
    } catch (error) {
      console.error('Error generating equal count bands for Parquet:', error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

// Complete calculateBandedDistribution function
async function calculateBandedDistribution(filePath, fileType, field, bandConfig) {
  console.log(`Calculating banded distribution for ${field}`);
  
  const sourceField = bandConfig.sourceField;
  const bands = bandConfig.bands;
  
  console.log(`Using source field ${sourceField} with ${bands.length} bands`);
  console.log('Band configurations:', bands);
  
  if (fileType === '.csv') {
    return new Promise((resolve, reject) => {
      // Count occurrences of each band
      const bandCounts = {};
      let totalProcessed = 0;
      let validValueCount = 0;
      let invalidValueCount = 0;
      
      // Initialize counts for each band
      bands.forEach(band => {
        bandCounts[band.name] = 0;
      });
      
      // Also track unassigned values
      bandCounts['Unassigned'] = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          totalProcessed++;
          
          // Skip if the source field value is missing
          if (!row[sourceField] && row[sourceField] !== 0) {
            invalidValueCount++;
            return;
          }
          
          // Parse the value
          let value;
          if (typeof row[sourceField] === 'number') {
            value = row[sourceField];
          } else if (typeof row[sourceField] === 'string') {
            value = parseFloat(row[sourceField].replace(/,/g, ''));
          } else {
            invalidValueCount++;
            return;
          }
          
          // Skip if not a valid number
          if (isNaN(value) || !isFinite(value)) {
            invalidValueCount++;
            return;
          }
          
          validValueCount++;
          
          // Find matching band
          let assigned = false;
          for (const band of bands) {
            if (!band || band.min === undefined || band.max === undefined) {
              console.warn(`Invalid band configuration: ${JSON.stringify(band)}`);
              continue;
            }
            
            const min = Number(band.min);
            const max = Number(band.max);
            
            if (value >= min && value <= max) {
              bandCounts[band.name] = (bandCounts[band.name] || 0) + 1;
              assigned = true;
              break; // Stop at first matching band
            }
          }
          
          if (!assigned) {
            bandCounts['Unassigned']++;
          }
        })
        .on('end', () => {
          console.log(`Processed ${totalProcessed} rows for ${field}`);
          console.log(`Found ${validValueCount} valid values and ${invalidValueCount} invalid values`);
          console.log('Band counts:', bandCounts);
          
          // Check if we have any data
          const totalCounts = Object.values(bandCounts).reduce((sum, count) => sum + count, 0);
          if (totalCounts === 0) {
            console.log(`No data found for banded distribution of ${field}`);
            resolve([{
              category: "No Data",
              count: 0
            }]);
            return;
          }
          
          // Format the data for the chart
          const categoricalData = Object.entries(bandCounts)
            .filter(([_, count]) => count > 0) // Remove empty categories
            .map(([category, count]) => ({
              category,
              count
            }))
            .sort((a, b) => {
              // First try to sort by band order if possible
              const bandA = bands.findIndex(band => band.name === a.category);
              const bandB = bands.findIndex(band => band.name === b.category);
              
              if (bandA !== -1 && bandB !== -1) {
                return bandA - bandB;
              }
              
              // If one is 'Unassigned', put it at the end
              if (a.category === 'Unassigned') return 1;
              if (b.category === 'Unassigned') return -1;
              
              // Otherwise sort by count descending
              return b.count - a.count;
            });
          
          console.log(`Returning ${categoricalData.length} categories for ${field}`);
          resolve(categoricalData);
        })
        .on('error', (err) => {
          console.error(`Error processing CSV for banded field ${field}:`, err);
          reject(err);
        });
    });
  } else if (fileType === '.parquet') {
    // Implementation for Parquet files
    try {
      console.log(`Opening parquet file for banded field ${field}: ${filePath}`);
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      // Count occurrences of each band
      const bandCounts = {};
      let totalProcessed = 0;
      let validValueCount = 0;
      
      // Initialize counts for each band
      bands.forEach(band => {
        bandCounts[band.name] = 0;
      });
      
      // Also track unassigned values
      bandCounts['Unassigned'] = 0;
      
      let row = null;
      while ((row = await cursor.next()) !== null) {
        totalProcessed++;
        
        // Skip if the source field value is missing
        if (!row[sourceField] && row[sourceField] !== 0) {
          continue;
        }
        
        // Parse the value
        let value;
        if (typeof row[sourceField] === 'number') {
          value = row[sourceField];
        } else if (typeof row[sourceField] === 'string') {
          value = parseFloat(row[sourceField].replace(/,/g, ''));
        } else {
          continue;
        }
        
        // Skip if not a valid number
        if (isNaN(value) || !isFinite(value)) {
          continue;
        }
        
        validValueCount++;
        
        // Find matching band
        let assigned = false;
        for (const band of bands) {
          if (!band || band.min === undefined || band.max === undefined) {
            console.warn(`Invalid band configuration: ${JSON.stringify(band)}`);
            continue;
          }
          
          const min = Number(band.min);
          const max = Number(band.max);
          
          if (value >= min && value <= max) {
            bandCounts[band.name] = (bandCounts[band.name] || 0) + 1;
            assigned = true;
            break; // Stop at first matching band
          }
        }
        
        if (!assigned) {
          bandCounts['Unassigned']++;
        }
      }
      
      await reader.close();
      
      console.log(`Processed ${totalProcessed} rows from parquet for ${field}`);
      console.log(`Found ${validValueCount} valid values`);
      console.log('Band counts:', bandCounts);
      
      // Check if we have any data
      const totalCounts = Object.values(bandCounts).reduce((sum, count) => sum + count, 0);
      if (totalCounts === 0) {
        console.log(`No data found for banded distribution of ${field} in parquet file`);
        return [{
          category: "No Data",
          count: 0
        }];
      }
      
      // Format the data for the chart
      const categoricalData = Object.entries(bandCounts)
        .filter(([_, count]) => count > 0) // Remove empty categories
        .map(([category, count]) => ({
          category,
          count
        }))
        .sort((a, b) => {
          // First try to sort by band order if possible
          const bandA = bands.findIndex(band => band.name === a.category);
          const bandB = bands.findIndex(band => band.name === b.category);
          
          if (bandA !== -1 && bandB !== -1) {
            return bandA - bandB;
          }
          
          // If one is 'Unassigned', put it at the end
          if (a.category === 'Unassigned') return 1;
          if (b.category === 'Unassigned') return -1;
          
          // Otherwise sort by count descending
          return b.count - a.count;
        });
      
      console.log(`Returning ${categoricalData.length} categories for ${field} from parquet`);
      return categoricalData;
    } catch (error) {
      console.error(`Error calculating banded distribution for Parquet file ${field}:`, error);
      throw error;
    }
  }
  
  throw new Error('Unsupported file type');
}

// Complete /api/banded-distribution endpoint
app.post('/api/banded-distribution', express.json(), async (req, res) => {
  try {
    const { fileId, field } = req.body;
    console.log(`Banded distribution request for field: ${field}, fileId: ${fileId}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      console.log(`Session not found for fileId: ${fileId}`);
      return res.status(404).json({ error: 'File session not found' });
    }
    
    console.log('Session derivedFields:', session.derivedFields);
    
    // Check if this is a derived banded field
    if (!session.derivedFields || !session.derivedFields[field]) {
      console.log(`No derived field config found for ${field}`);
      return res.status(400).json({ error: `${field} is not a derived field` });
    }
    
    const fieldConfig = session.derivedFields[field];
    console.log(`Found derived field config for ${field}:`, fieldConfig);
    
    if (fieldConfig.type !== 'banded') {
      console.log(`${field} is not a banded field (type: ${fieldConfig.type})`);
      return res.status(400).json({ error: `${field} is not a banded field` });
    }
    
    const filePath = session.filePath;
    const fileType = session.fileType;
    
    // Calculate banded distribution
    console.log(`Calculating banded distribution for ${field}`);
    const categoricalData = await calculateBandedDistribution(
      filePath, 
      fileType,
      field,
      fieldConfig
    );
    
    console.log(`Returning ${categoricalData.length} categories for ${field}`);
    res.json({ categoricalData });
  } catch (error) {
    console.error('Error calculating banded distribution:', error);
    res.status(500).json({ error: 'Failed to calculate banded distribution: ' + error.message });
  }
});

// Complete /api/field-stats endpoint
app.post('/api/field-stats', express.json(), async (req, res) => {
  try {
    const { fileId, fieldName } = req.body;
    console.log(`Calculating field stats for ${fieldName}, fileId: ${fileId}`);
    
    const session = fileSessions.get(fileId);
    if (!session) {
      console.log(`Session not found for fileId: ${fileId}`);
      return res.status(404).json({ error: 'File session not found' });
    }
    
    // Check if this is a derived field
    const isDerivedField = session.derivedFields && session.derivedFields[fieldName];
    console.log(`Is derived field: ${isDerivedField}`);
    
    if (isDerivedField) {
      console.log('Derived field config:', session.derivedFields[fieldName]);
    }
    
    // Calculate stats for the specified field
    const stats = await calculateFieldStats(
      session.filePath,
      session.fileType,
      fieldName,
      isDerivedField ? session.derivedFields[fieldName] : null
    );
    
    console.log(`Calculated stats for ${fieldName}:`, stats);
    res.json({ stats });
  } catch (error) {
    console.error('Error calculating field stats:', error);
    res.status(500).json({ error: 'Failed to calculate field stats: ' + error.message });
  }
});

// Cleanup unused session data (run periodically)
function cleanupSessions() {
  const now = Date.now();
  const maxAge = 24 * 60 * 60 * 1000; // 24 hours
  
  fileSessions.forEach((session, fileId) => {
    const sessionTime = parseInt(fileId.split('-')[0]);
    if (now - sessionTime > maxAge) {
      // Remove temporary file
      if (session.filePath && fs.existsSync(session.filePath)) {
        try {
          fs.unlinkSync(session.filePath);
        } catch (error) {
          console.error(`Error deleting temp file ${session.filePath}:`, error);
        }
      }
      
      // Remove session
      fileSessions.delete(fileId);
    }
  });
}

// Run cleanup every hour
setInterval(cleanupSessions, 60 * 60 * 1000);

// Add file verification utility endpoint
app.get('/api/verify-file/:fileId', (req, res) => {
  try {
    const { fileId } = req.params;
    const session = fileSessions.get(fileId);
    
    if (!session) {
      return res.status(404).json({ error: 'File session not found' });
    }
    
    if (!fs.existsSync(session.filePath)) {
      return res.status(404).json({ error: 'File not found on server' });
    }
    
    const fileSize = fs.statSync(session.filePath).size;
    
    res.json({
      fileId,
      filePath: path.basename(session.filePath),
      fileType: session.fileType,
      fileSize,
      uploadTime: new Date(parseInt(fileId.split('-')[0])).toISOString()
    });
  } catch (error) {
    console.error('Error verifying file:', error);
    res.status(500).json({ error: 'Failed to verify file: ' + error.message });
  }
});

// Cleanup API endpoint
app.get('/api/cleanup-files', (req, res) => {
  console.log("Manual cleanup triggered via API");
  
  try {
    cleanupAllFiles();
    res.json({ 
      success: true, 
      message: 'All files have been cleaned up successfully'
    });
  } catch (error) {
    console.error('Error during manual cleanup:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to clean up files',
      message: error.message
    });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
  
  // Clean up all existing files first
  cleanupAllFiles();
  
  // Create datasets directory if it doesn't exist
  const datasetsDir = path.join(__dirname, 'datasets');
  if (!fs.existsSync(datasetsDir)) {
    fs.mkdirSync(datasetsDir, { recursive: true });
    console.log(`Created datasets directory: ${datasetsDir}`);
  }
  
  // Create uploads directory if it doesn't exist
  const uploadsDir = path.join(__dirname, 'uploads');
  if (!fs.existsSync(uploadsDir)) {
    fs.mkdirSync(uploadsDir, { recursive: true });
    console.log(`Created uploads directory: ${uploadsDir}`);
  }
});
              