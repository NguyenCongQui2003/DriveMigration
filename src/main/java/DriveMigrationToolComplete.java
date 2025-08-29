import javax.swing.*;
import javax.swing.table.*;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.filechooser.*;

public class DriveMigrationToolComplete extends JFrame {
    // Services và configuration
    private GoogleSheetsServiceComplete sheetsService;
    private DriveServiceComplete driveService;
    private String serviceAccountJsonPath = "";
    private String privateKeyContent = "";

    // Threading
    private ExecutorService executor;
    private volatile boolean migrationPaused = false;
    private volatile boolean migrationStopped = false;
    private final AtomicInteger runningTasks = new AtomicInteger(0);

    // UI Components
    private JTable userTable;
    private UserTableModel userTableModel;
    private JTextArea logArea;
    private JProgressBar overallProgress;
    private JLabel statusLabel;
    private JSpinner threadCountSpinner;
    private JButton startButton, stopButton, pauseButton;
    private JCheckBox autoRetryCheckBox;
    private JSpinner retryCountSpinner;

    // Statistics labels
    private JLabel totalUsersLabel, completedLabel, failedLabel, inProgressLabel;
    private JLabel totalFilesLabel, successFilesLabel, restrictedFilesLabel;

    // Configuration fields
    private JTextField serviceAccountEmailField;
    private JTextField spreadsheetIdField;

    // Migration statistics
    private final AtomicInteger totalProcessedFiles = new AtomicInteger(0);
    private final AtomicInteger totalSuccessFiles = new AtomicInteger(0);
    private final AtomicInteger totalFailedFiles = new AtomicInteger(0);
    private final AtomicInteger totalRestrictedFiles = new AtomicInteger(0);

    public DriveMigrationToolComplete() {
        initializeUI();
        setupEventHandlers();
        loadConfiguration();
    }

    private void initializeUI() {
        setTitle("Drive Permission Migration Tool - Enhanced Version");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1400, 900);
        setLocationRelativeTo(null);

        setLayout(new BorderLayout());
        add(createConfigPanel(), BorderLayout.NORTH);
        add(createMainPanel(), BorderLayout.CENTER);
        add(createControlPanel(), BorderLayout.SOUTH);
        setJMenuBar(createMenuBar());
    }

    private JPanel createConfigPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(new TitledBorder("Service Account Configuration"));
        panel.setBackground(new Color(240, 248, 255));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);

        // Service Account JSON File
        gbc.gridx = 0; gbc.gridy = 0;
        panel.add(new JLabel("Service Account JSON:"), gbc);

        gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.weightx = 1.0;
        JTextField jsonFilePathField = new JTextField(50);
        jsonFilePathField.setEditable(false);
        jsonFilePathField.setText("No file selected");
        panel.add(jsonFilePathField, gbc);

        gbc.gridx = 2; gbc.fill = GridBagConstraints.NONE; gbc.weightx = 0;
        JButton browseButton = new JButton("Browse...");
        browseButton.addActionListener(e -> {
            JFileChooser fileChooser = new JFileChooser();
            fileChooser.setFileFilter(new FileNameExtensionFilter("JSON Files", "json"));

            if (fileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
                File selectedFile = fileChooser.getSelectedFile();
                serviceAccountJsonPath = selectedFile.getAbsolutePath();
                jsonFilePathField.setText(selectedFile.getName());
                jsonFilePathField.setToolTipText(serviceAccountJsonPath);

                // Extract email and private key from JSON
                extractCredentialsFromJson(selectedFile);
            }
        });
        panel.add(browseButton, gbc);

        // Service Account Email
        gbc.gridx = 0; gbc.gridy = 1; gbc.weightx = 0;
        panel.add(new JLabel("Service Account Email:"), gbc);
        gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.weightx = 1.0;
        serviceAccountEmailField = new JTextField(50);
        serviceAccountEmailField.setEditable(false);
        serviceAccountEmailField.setBackground(new Color(245, 245, 245));
        panel.add(serviceAccountEmailField, gbc);

        // Private Key Status
        gbc.gridx = 2; gbc.fill = GridBagConstraints.NONE; gbc.weightx = 0;
        JButton privateKeyButton = new JButton("View Key");
        privateKeyButton.addActionListener(e -> showPrivateKeyDialog());
        panel.add(privateKeyButton, gbc);

        // Spreadsheet ID
        gbc.gridx = 0; gbc.gridy = 2; gbc.weightx = 0; gbc.gridwidth = 1;
        panel.add(new JLabel("Spreadsheet ID:"), gbc);

        gbc.gridx = 1; gbc.weightx = 1.0; gbc.fill = GridBagConstraints.HORIZONTAL;
        spreadsheetIdField = new JTextField(100);
        panel.add(spreadsheetIdField, gbc);


        return panel;
    }

    private JPanel createMainPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                createUserTablePanel(), createStatsAndLogsPanel());
        splitPane.setDividerLocation(700);
        splitPane.setResizeWeight(0.6);

        panel.add(splitPane, BorderLayout.CENTER);
        return panel;
    }

    private JPanel createUserTablePanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new TitledBorder("Users Migration Status"));

        // Create table
        userTableModel = new UserTableModel();
        userTable = new JTable(userTableModel);
        userTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        userTable.setRowHeight(25);

        // Custom renderers
        userTable.setDefaultRenderer(String.class, new StatusCellRenderer());
        userTable.setDefaultRenderer(Integer.class, new NumberCellRenderer());
        userTable.setDefaultRenderer(JProgressBar.class, new ProgressBarRenderer());

        // Column widths
        TableColumnModel columnModel = userTable.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(200); // Email
        columnModel.getColumn(1).setPreferredWidth(120); // Status
        columnModel.getColumn(2).setPreferredWidth(100); // Progress
        columnModel.getColumn(3).setPreferredWidth(80);  // Total Files
        columnModel.getColumn(4).setPreferredWidth(80);  // Success
        columnModel.getColumn(5).setPreferredWidth(80);  // Failed
        columnModel.getColumn(6).setPreferredWidth(80);  // Restricted

        JScrollPane scrollPane = new JScrollPane(userTable);
        panel.add(scrollPane, BorderLayout.CENTER);

        // Toolbar
        JPanel toolbar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton loadUsersButton = new JButton("Load Users");
        JButton refreshButton = new JButton("Refresh");
        JButton exportButton = new JButton("Export Results");

        loadUsersButton.addActionListener(e -> loadUsers());
        refreshButton.addActionListener(e -> refreshUserTable());
        exportButton.addActionListener(e -> exportResults());

        toolbar.add(loadUsersButton);
        toolbar.add(refreshButton);
        toolbar.add(exportButton);
        panel.add(toolbar, BorderLayout.NORTH);

        return panel;
    }

    private JPanel createStatsAndLogsPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
                createStatisticsPanel(), createLogsPanel());
        splitPane.setDividerLocation(250);
        splitPane.setResizeWeight(0.4);

        panel.add(splitPane, BorderLayout.CENTER);
        return panel;
    }

    private JPanel createStatisticsPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(new TitledBorder("Migration Statistics"));
        panel.setBackground(new Color(245, 245, 245));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 10, 5, 10);
        gbc.anchor = GridBagConstraints.WEST;

        // User statistics
        gbc.gridx = 0; gbc.gridy = 0;
        panel.add(new JLabel("Total Users:"), gbc);
        gbc.gridx = 1;
        totalUsersLabel = new JLabel("0");
        panel.add(totalUsersLabel, gbc);

        gbc.gridx = 0; gbc.gridy = 1;
        panel.add(new JLabel("Completed:"), gbc);
        gbc.gridx = 1;
        completedLabel = new JLabel("0");
        completedLabel.setForeground(Color.GREEN.darker());
        panel.add(completedLabel, gbc);

        gbc.gridx = 0; gbc.gridy = 2;
        panel.add(new JLabel("In Progress:"), gbc);
        gbc.gridx = 1;
        inProgressLabel = new JLabel("0");
        inProgressLabel.setForeground(Color.BLUE);
        panel.add(inProgressLabel, gbc);

        gbc.gridx = 0; gbc.gridy = 3;
        panel.add(new JLabel("Failed:"), gbc);
        gbc.gridx = 1;
        failedLabel = new JLabel("0");
        failedLabel.setForeground(Color.RED);
        panel.add(failedLabel, gbc);

        // File statistics
        gbc.gridx = 2; gbc.gridy = 0;
        panel.add(new JLabel("Total Files:"), gbc);
        gbc.gridx = 3;
        totalFilesLabel = new JLabel("0");
        panel.add(totalFilesLabel, gbc);

        gbc.gridx = 2; gbc.gridy = 1;
        panel.add(new JLabel("Success Files:"), gbc);
        gbc.gridx = 3;
        successFilesLabel = new JLabel("0");
        successFilesLabel.setForeground(Color.GREEN.darker());
        panel.add(successFilesLabel, gbc);

        gbc.gridx = 2; gbc.gridy = 2;
        panel.add(new JLabel("Restricted Files:"), gbc);
        gbc.gridx = 3;
        restrictedFilesLabel = new JLabel("0");
        restrictedFilesLabel.setForeground(Color.ORANGE.darker());
        panel.add(restrictedFilesLabel, gbc);

        // Overall progress
        gbc.gridx = 0; gbc.gridy = 4; gbc.gridwidth = 4; gbc.fill = GridBagConstraints.HORIZONTAL;
        overallProgress = new JProgressBar(0, 100);
        overallProgress.setStringPainted(true);
        overallProgress.setString("Ready");
        panel.add(overallProgress, gbc);

        return panel;
    }

    private JPanel createLogsPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new TitledBorder("Migration Logs"));

        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        logArea.setBackground(Color.BLACK);
        logArea.setForeground(Color.GREEN);

        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        panel.add(scrollPane, BorderLayout.CENTER);

        // Log toolbar
        JPanel toolbar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton clearLogsButton = new JButton("Clear Logs");
        JButton saveLogsButton = new JButton("Save Logs");

        clearLogsButton.addActionListener(e -> logArea.setText(""));
        saveLogsButton.addActionListener(e -> saveLogs());

        toolbar.add(clearLogsButton);
        toolbar.add(saveLogsButton);
        panel.add(toolbar, BorderLayout.NORTH);

        return panel;
    }

    private JPanel createControlPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));

        // Left - Controls
        JPanel controlsPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));

        controlsPanel.add(new JLabel("Threads:"));
        threadCountSpinner = new JSpinner(new SpinnerNumberModel(5, 1, 20, 1));
        controlsPanel.add(threadCountSpinner);

        controlsPanel.add(Box.createHorizontalStrut(20));
        autoRetryCheckBox = new JCheckBox("Auto Retry", true);
        controlsPanel.add(autoRetryCheckBox);

        controlsPanel.add(new JLabel("Max Retries:"));
        retryCountSpinner = new JSpinner(new SpinnerNumberModel(3, 0, 10, 1));
        controlsPanel.add(retryCountSpinner);

        // Action buttons
        controlsPanel.add(Box.createHorizontalStrut(30));
        startButton = new JButton("Start Migration");
        startButton.setBackground(new Color(76, 175, 80));
        startButton.setForeground(Color.black);

        pauseButton = new JButton("Pause");
        pauseButton.setBackground(new Color(255, 193, 7));
        pauseButton.setForeground(Color.black);
        pauseButton.setEnabled(false);

        stopButton = new JButton("Stop");
        stopButton.setBackground(new Color(244, 67, 54));
        stopButton.setForeground(Color.black);
        stopButton.setEnabled(false);

        controlsPanel.add(startButton);
        controlsPanel.add(pauseButton);
        controlsPanel.add(stopButton);

        // Right - Status
        JPanel statusPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        statusLabel = new JLabel("Ready");
        statusLabel.setFont(statusLabel.getFont().deriveFont(Font.BOLD));
        statusPanel.add(statusLabel);

        panel.add(controlsPanel, BorderLayout.WEST);
        panel.add(statusPanel, BorderLayout.EAST);

        return panel;
    }

    private JMenuBar createMenuBar() {
        JMenuBar menuBar = new JMenuBar();

        // File menu
        JMenu fileMenu = new JMenu("File");
        fileMenu.add(new JMenuItem("Settings")).addActionListener(e -> showSettings());
        fileMenu.addSeparator();
        fileMenu.add(new JMenuItem("Exit")).addActionListener(e -> System.exit(0));

        // Tools menu
        JMenu toolsMenu = new JMenu("Tools");
        toolsMenu.add(new JMenuItem("Test Connection")).addActionListener(e -> testConnection());
        toolsMenu.add(new JMenuItem("Validate Config")).addActionListener(e -> validateConfig());

        // Help menu
        JMenu helpMenu = new JMenu("Help");
        helpMenu.add(new JMenuItem("Setup Guide")).addActionListener(e -> showSetupGuide());
        helpMenu.add(new JMenuItem("About")).addActionListener(e -> showAbout());

        menuBar.add(fileMenu);
        menuBar.add(toolsMenu);
        menuBar.add(helpMenu);

        return menuBar;
    }

    private void setupEventHandlers() {
        startButton.addActionListener(e -> startMigration());
        pauseButton.addActionListener(e -> pauseMigration());
        stopButton.addActionListener(e -> stopMigration());

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                if (executor != null && !executor.isShutdown()) {
                    int choice = JOptionPane.showConfirmDialog(
                            DriveMigrationToolComplete.this,
                            "Migration is in progress. Exit anyway?",
                            "Confirm Exit",
                            JOptionPane.YES_NO_OPTION
                    );
                    if (choice == JOptionPane.YES_OPTION) {
                        if (executor != null) executor.shutdownNow();
                        System.exit(0);
                    }
                } else {
                    System.exit(0);
                }
            }
        });
    }

    // Core functionality methods
    private void extractCredentialsFromJson(File jsonFile) {
        try {
            String jsonContent = new String(java.nio.file.Files.readAllBytes(jsonFile.toPath()));

            // Extract client_email
            String emailRegex = "\"client_email\"\\s*:\\s*\"([^\"]+)\"";
            java.util.regex.Pattern emailPattern = java.util.regex.Pattern.compile(emailRegex);
            java.util.regex.Matcher emailMatcher = emailPattern.matcher(jsonContent);

            if (emailMatcher.find()) {
                serviceAccountEmailField.setText(emailMatcher.group(1));
                appendLog("✓ Đã tải email service account: " + emailMatcher.group(1));
            }

            // Extract private_key
            String keyRegex = "\"private_key\"\\s*:\\s*\"([^\"]+)\"";
            java.util.regex.Pattern keyPattern = java.util.regex.Pattern.compile(keyRegex);
            java.util.regex.Matcher keyMatcher = keyPattern.matcher(jsonContent);

            if (keyMatcher.find()) {
                privateKeyContent = keyMatcher.group(1).replace("\\n", "\n");
                appendLog("✓ Đã tải private key thành công");
            }

        } catch (Exception e) {
            appendLog("✗ Lỗi đọc file JSON: " + e.getMessage());
            JOptionPane.showMessageDialog(this,
                    "Error reading JSON file: " + e.getMessage(),
                    "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void loadUsers() {
        if (!validateConfiguration()) return;

        try {
            appendLog("🔄 Đang tải danh sách người dùng từ Google Sheets...");

            if (sheetsService == null) {
                sheetsService = new GoogleSheetsServiceComplete(
                        serviceAccountEmailField.getText().trim(),
                        privateKeyContent,
                        spreadsheetIdField.getText().trim()
                );
            }

            List<UserRecord> users = sheetsService.getUserList();

            userTableModel.clearUsers();
            for (UserRecord user : users) {
                userTableModel.addUser(user);
            }

            appendLog(String.format("✓ Đã tải %d người dùng thành công", users.size()));
            updateStatistics();

        } catch (Exception e) {
            appendLog("✗ Lỗi khi tải danh sách người dùng: " + e.getMessage());
            JOptionPane.showMessageDialog(this,
                    "Lỗi khi tải danh sách người dùng: " + e.getMessage(),
                    "Lỗi", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void startMigration() {
        if (!validateConfiguration()) return;

        try {
            appendLog("🚀 Đang bắt đầu migration quyền Drive...");

            // Initialize services
            if (sheetsService == null) {
                sheetsService = new GoogleSheetsServiceComplete(
                        serviceAccountEmailField.getText().trim(),
                        privateKeyContent,
                        spreadsheetIdField.getText().trim()
                );
            }

            if (driveService == null) {
                driveService = new DriveServiceComplete(
                        serviceAccountEmailField.getText().trim(),
                        privateKeyContent
                );
            }

            // Get users and mapping
            List<UserRecord> users = sheetsService.getUserList();
            Map<String, String> userMapping = sheetsService.getUserMapping();

            // Filter pending users
            List<UserRecord> pendingUsers = new ArrayList<>();
            for (UserRecord user : users) {
                if ("Not Started".equals(user.status) || "In Progress".equals(user.status)) {
                    pendingUsers.add(user);
                }
            }

            if (pendingUsers.isEmpty()) {
                JOptionPane.showMessageDialog(this, "No users need processing.",
                        "Information", JOptionPane.INFORMATION_MESSAGE);
                return;
            }

            // Setup execution
            int threadCount = (Integer) threadCountSpinner.getValue();
            executor = Executors.newFixedThreadPool(threadCount);
            migrationPaused = false;
            migrationStopped = false;

            // Update UI
            startButton.setEnabled(false);
            pauseButton.setEnabled(true);
            stopButton.setEnabled(true);
            statusLabel.setText("Migration in progress...");

            appendLog(String.format("📊 Processing %d users with %d threads",
                    pendingUsers.size(), threadCount));

            // Reset statistics
            totalProcessedFiles.set(0);
            totalSuccessFiles.set(0);
            totalFailedFiles.set(0);
            totalRestrictedFiles.set(0);

            // Submit migration tasks
            for (UserRecord user : pendingUsers) {
                executor.submit(() -> processUser(user, userMapping));
            }

            // Monitor completion
            executor.submit(() -> {
                executor.shutdown();
                try {
                    if (executor.awaitTermination(24, TimeUnit.HOURS)) {
                        SwingUtilities.invokeLater(() -> {
                            startButton.setEnabled(true);
                            pauseButton.setEnabled(false);
                            stopButton.setEnabled(false);
                            statusLabel.setText("Migration completed");
                            appendLog("🎉 Migration completed successfully!");
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

        } catch (Exception e) {
            appendLog("✗ Error starting migration: " + e.getMessage());
            JOptionPane.showMessageDialog(this,
                    "Error starting migration: " + e.getMessage(),
                    "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void processUser(UserRecord user, Map<String, String> userMapping) {
        if (migrationStopped) return;

        try {
            // Wait if paused
            while (migrationPaused && !migrationStopped) {
                Thread.sleep(1000);
            }

            if (migrationStopped) return;

            SwingUtilities.invokeLater(() -> {
                updateUserStatus(user.email, "In Progress");
                appendLog("👤 Processing user: " + user.email);
            });

            // Update status in Google Sheets
            sheetsService.updateUserStatus(user.email, user.rowIndex, "In Progress", null);

            // Process user's drive
            MigrationResult result = driveService.processUserDrive(user.email, userMapping,
                    (email, current, total, fileResult) -> {
                        SwingUtilities.invokeLater(() -> {
                            updateUserProgress(email, current, total);
                            updateFileStatistics(fileResult);
                        });
                    });

            // Update final results
            SwingUtilities.invokeLater(() -> {
                String status = result.success ? "Completed" : "Failed";
                updateUserStatus(user.email, status);
                updateUserStats(user.email, result);

                appendLog(String.format("✓ User %s: %d files processed (%d success, %d failed, %d restricted)",
                        user.email, result.totalFiles, result.successFiles,
                        result.failedFiles, result.restrictedFiles));
            });

            // Update Google Sheets
            MigrationStats stats = new MigrationStats();
            stats.totalFiles = result.totalFiles;
            stats.successFiles = result.successFiles;
            stats.failedFiles = result.failedFiles;
            stats.restrictedFiles = result.restrictedFiles;

            sheetsService.updateUserStatus(user.email, user.rowIndex,
                    result.success ? "Completed" : "Failed", stats);
            sheetsService.createOrUpdateUserDetailSheet(user.email, result.fileResults);

        } catch (Exception e) {
            SwingUtilities.invokeLater(() -> {
                updateUserStatus(user.email, "Failed");
                appendLog("✗ Error processing user " + user.email + ": " + e.getMessage());
            });

            try {
                sheetsService.updateUserStatus(user.email, user.rowIndex, "Failed", null);
            } catch (Exception ex) {
                // Ignore
            }
        }
    }

    private void pauseMigration() {
        migrationPaused = !migrationPaused;
        if (migrationPaused) {
            pauseButton.setText("Resume");
            statusLabel.setText("Migration paused");
            appendLog("⏸ Migration paused");
        } else {
            pauseButton.setText("Pause");
            statusLabel.setText("Migration resumed");
            appendLog("▶ Migration resumed");
        }
    }

    private void stopMigration() {
        migrationStopped = true;
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        startButton.setEnabled(true);
        pauseButton.setEnabled(false);
        stopButton.setEnabled(false);
        statusLabel.setText("Migration stopped");
        appendLog("⏹ Migration stopped by user");
    }

    // Helper methods
    private boolean validateConfiguration() {
        StringBuilder errors = new StringBuilder();

        if (serviceAccountJsonPath.trim().isEmpty()) {
            errors.append("- Service Account JSON file required\n");
        }
        if (serviceAccountEmailField.getText().trim().isEmpty()) {
            errors.append("- Service Account Email required\n");
        }
        if (privateKeyContent.trim().isEmpty()) {
            errors.append("- Private Key required\n");
        }
        if (spreadsheetIdField.getText().trim().isEmpty()) {
            errors.append("- Spreadsheet ID required\n");
        }

        if (errors.length() > 0) {
            JOptionPane.showMessageDialog(this,
                    "Configuration errors:\n" + errors.toString(),
                    "Validation Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }

        return true;
    }

    private void updateUserStatus(String email, String status) {
        for (int i = 0; i < userTableModel.getRowCount(); i++) {
            UserRecord user = userTableModel.getUser(i);
            if (user != null && user.email.equals(email)) {
                user.status = status;
                if ("In Progress".equals(status)) {
                    user.startTime = LocalDateTime.now();
                } else if ("Completed".equals(status) || "Failed".equals(status)) {
                    user.endTime = LocalDateTime.now();
                }
                userTableModel.fireTableRowsUpdated(i, i);
                updateStatistics();
                break;
            }
        }
    }

    private void updateUserProgress(String email, int current, int total) {
        for (int i = 0; i < userTableModel.getRowCount(); i++) {
            UserRecord user = userTableModel.getUser(i);
            if (user != null && user.email.equals(email)) {
                int progress = (int) ((double) current / total * 100);
                user.progressBar.setValue(progress);
                user.progressBar.setString(String.format("%d/%d (%d%%)", current, total, progress));
                userTableModel.fireTableRowsUpdated(i, i);
                break;
            }
        }
    }

    private void updateUserStats(String email, MigrationResult result) {
        for (int i = 0; i < userTableModel.getRowCount(); i++) {
            UserRecord user = userTableModel.getUser(i);
            if (user != null && user.email.equals(email)) {
                user.totalFiles = result.totalFiles;
                user.successFiles = result.successFiles;
                user.failedFiles = result.failedFiles;
                user.restrictedFiles = result.restrictedFiles;
                userTableModel.fireTableRowsUpdated(i, i);
                break;
            }
        }
    }

    private void updateFileStatistics(FileProcessingResult result) {
        totalProcessedFiles.incrementAndGet();
        switch (result.status) {
            case "SUCCESS":
                totalSuccessFiles.incrementAndGet();
                break;
            case "ERROR":
                totalFailedFiles.incrementAndGet();
                break;
            case "RESTRICTED":
                totalRestrictedFiles.incrementAndGet();
                break;
        }
        updateStatistics();
    }

    private void updateStatistics() {
        int totalUsers = userTableModel.getRowCount();
        int completed = 0, inProgress = 0, failed = 0;

        for (int i = 0; i < totalUsers; i++) {
            UserRecord user = userTableModel.getUser(i);
            if (user != null) {
                switch (user.status) {
                    case "Completed": completed++; break;
                    case "In Progress": inProgress++; break;
                    case "Failed": failed++; break;
                }
            }
        }

        totalUsersLabel.setText(String.valueOf(totalUsers));
        completedLabel.setText(String.valueOf(completed));
        inProgressLabel.setText(String.valueOf(inProgress));
        failedLabel.setText(String.valueOf(failed));

        totalFilesLabel.setText(String.valueOf(totalProcessedFiles.get()));
        successFilesLabel.setText(String.valueOf(totalSuccessFiles.get()));
        restrictedFilesLabel.setText(String.valueOf(totalRestrictedFiles.get()));

        if (totalUsers > 0) {
            int progress = (int) ((double) (completed + failed) / totalUsers * 100);
            overallProgress.setValue(progress);
            overallProgress.setString(String.format("%d%% (%d/%d users)",
                    progress, completed + failed, totalUsers));
        }
    }

    private void appendLog(String message) {
        SwingUtilities.invokeLater(() -> {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            logArea.append("[" + timestamp + "] " + message + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    // Menu action methods
    private void refreshUserTable() { loadUsers(); }

    private void exportResults() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setSelectedFile(new File("migration-results.csv"));

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            try (PrintWriter writer = new PrintWriter(fileChooser.getSelectedFile())) {
                writer.println("Email,Status,Total Files,Success Files,Failed Files,Restricted Files");

                for (int i = 0; i < userTableModel.getRowCount(); i++) {
                    UserRecord user = userTableModel.getUser(i);
                    if (user != null) {
                        writer.printf("%s,%s,%d,%d,%d,%d%n",
                                user.email, user.status, user.totalFiles,
                                user.successFiles, user.failedFiles, user.restrictedFiles);
                    }
                }

                appendLog("✓ Results exported to: " + fileChooser.getSelectedFile().getName());
            } catch (Exception e) {
                appendLog("✗ Error exporting results: " + e.getMessage());
            }
        }
    }

    private void showPrivateKeyDialog() {
        JDialog dialog = new JDialog(this, "Private Key", true);
        dialog.setSize(600, 400);
        dialog.setLocationRelativeTo(this);

        JTextArea keyArea = new JTextArea(privateKeyContent);
        keyArea.setEditable(false);
        keyArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 10));

        JScrollPane scrollPane = new JScrollPane(keyArea);
        dialog.add(scrollPane, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout());
        JButton closeButton = new JButton("Close");
        closeButton.addActionListener(e -> dialog.dispose());
        buttonPanel.add(closeButton);
        dialog.add(buttonPanel, BorderLayout.SOUTH);

        dialog.setVisible(true);
    }

    private void showSettings() {
        int result = JOptionPane.showConfirmDialog(this,
                "Save current configuration?", "Settings", JOptionPane.YES_NO_OPTION);
        if (result == JOptionPane.YES_OPTION) {
            saveConfiguration();
        }
    }

    private void testConnection() {
        if (!validateConfiguration()) return;

        try {
            appendLog("🔧 Đang kiểm tra kết nối...");

            // Test Google Sheets
            GoogleSheetsServiceComplete testSheets = new GoogleSheetsServiceComplete(
                    serviceAccountEmailField.getText().trim(),
                    privateKeyContent,
                    spreadsheetIdField.getText().trim()
            );

            List<UserRecord> users = testSheets.getUserList();
            appendLog("✓ Kết nối Google Sheets thành công (" + users.size() + " người dùng)");

            // Test user mapping
            Map<String, String> userMapping = testSheets.getUserMapping();
            appendLog("✓ Đã tải mapping người dùng (" + userMapping.size() + " ánh xạ)");

            // Debug: Print mapping
            for (Map.Entry<String, String> entry : userMapping.entrySet()) {
                appendLog("  Ánh xạ: " + entry.getKey() + " -> " + entry.getValue());
            }

            // Test Drive service with first user
            if (!users.isEmpty()) {
                DriveServiceComplete testDrive = new DriveServiceComplete(
                        serviceAccountEmailField.getText().trim(),
                        privateKeyContent
                );

                String testUser = users.get(0).email;
                appendLog("Đang kiểm tra truy cập Drive cho người dùng: " + testUser);

                try {
                    appendLog("🔍 Đang test authentication cho user: " + testUser);
                    boolean driveTest = testDrive.testConnection(testUser);
                    appendLog(driveTest ? "✓ Kết nối Drive service thành công" :
                            "✗ Kết nối Drive service thất bại");

                    if (driveTest) {
                        // Test getting files for first user
                        try {
                            appendLog("Đang kiểm tra lấy danh sách file cho: " + testUser);
                            List<DriveFile> files = testDrive.getAllFiles(testUser);
                            appendLog("✓ Tìm thấy " + files.size() + " file cho người dùng: " + testUser);

                            // Show first few files
                            for (int i = 0; i < Math.min(3, files.size()); i++) {
                                DriveFile file = files.get(i);
                                appendLog("  File " + (i+1) + ": " + file.name + " (" + file.permissions.size() + " quyền)");
                            }

                        } catch (Exception e) {
                            appendLog("✗ Lỗi khi lấy danh sách file: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    appendLog("✗ Lỗi khi kiểm tra Drive service: " + e.getMessage());
                    appendLog("Chi tiết lỗi: " + e.getClass().getSimpleName());

                    // Print stack trace for debugging
                    if (e.getCause() != null) {
                        appendLog("Nguyên nhân: " + e.getCause().getMessage());
                    }

                    if (e.getMessage().contains("Failed to get access token")) {
                        appendLog("⚠️ Có thể do:");
                        appendLog("  - Service Account chưa enable Domain-wide Delegation trong Cloud Console");
                        appendLog("  - APIs chưa được enable (Drive API, Sheets API)");
                        appendLog("  - Cần đợi 5-15 phút để thay đổi có hiệu lực");
                        appendLog("  - Client ID trong Admin Console không khớp với JSON file");
                    }

                    if (e.getMessage().contains("401") || e.getMessage().contains("unauthorized")) {
                        appendLog("🔑 Lỗi xác thực - kiểm tra:");
                        appendLog("  - Domain-wide Delegation đã được enable chưa?");
                        appendLog("  - Scopes có đúng không?");
                        appendLog("  - User có tồn tại trong domain không?");
                    }
                }
            }

            JOptionPane.showMessageDialog(this, "Kiểm tra kết nối hoàn tất. Xem logs để biết chi tiết.",
                    "Kết quả kiểm tra", JOptionPane.INFORMATION_MESSAGE);

        } catch (Exception e) {
            appendLog("✗ Kiểm tra kết nối thất bại: " + e.getMessage());
            e.printStackTrace();
            JOptionPane.showMessageDialog(this, "Kiểm tra kết nối thất bại: " + e.getMessage(),
                    "Kiểm tra thất bại", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void validateConfig() {
        if (validateConfiguration()) {
            JOptionPane.showMessageDialog(this, "Configuration is valid!",
                    "Validation Success", JOptionPane.INFORMATION_MESSAGE);
            appendLog("✓ Configuration validation successful");
        }
    }

    private void showSetupGuide() {
        String guideText = "🔧 HƯỚNG DẪN SETUP DRIVE MIGRATION TOOL\n\n" +
                "1. TẠO SERVICE ACCOUNT:\n" +
                "   • Vào Google Cloud Console (console.cloud.google.com)\n" +
                "   • Tạo project mới hoặc chọn project hiện có\n" +
                "   • Enable Google Drive API và Google Sheets API\n" +
                "   • Tạo Service Account trong IAM & Admin\n" +
                "   • Tải xuống JSON key file\n\n" +
                "2. CẤU HÌNH DOMAIN-WIDE DELEGATION:\n" +
                "   • Trong Service Account, enable Domain-wide Delegation\n" +
                "   • Copy Client ID từ JSON file\n" +
                "   • Vào Google Admin Console (admin.google.com)\n" +
                "   • Security → API Controls → Domain-wide Delegation\n" +
                "   • Add new với Client ID và scopes:\n" +
                "     https://www.googleapis.com/auth/drive\n" +
                "     https://www.googleapis.com/auth/drive.file\n" +
                "     https://www.googleapis.com/auth/spreadsheets\n\n" +
                "3. CHUẨN BỊ GOOGLE SHEETS:\n" +
                "   • Sheet1: Danh sách email users (cột A)\n" +
                "   • Sheet2: Mapping old email → new email (cột A, B)\n\n" +
                "4. SỬ DỤNG TOOL:\n" +
                "   • Browse và chọn Service Account JSON file\n" +
                "   • Nhập Spreadsheet ID\n" +
                "   • Click 'Load Users' để tải danh sách\n" +
                "   • Click 'Test Connection' để kiểm tra\n" +
                "   • Click 'Start Migration' để bắt đầu\n\n" +
                "⚠️ LƯU Ý:\n" +
                "• Tool sẽ THÊM quyền mới, không xóa quyền cũ\n" +
                "• Cần quyền Admin để setup Domain-wide Delegation\n" +
                "• Nên test với ít users trước khi chạy full migration";

        JTextArea textArea = new JTextArea(guideText);
        textArea.setEditable(false);
        textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        textArea.setBackground(getBackground());

        JScrollPane scrollPane = new JScrollPane(textArea);
        scrollPane.setPreferredSize(new Dimension(600, 500));

        JOptionPane.showMessageDialog(this, scrollPane, "Hướng dẫn Setup", JOptionPane.INFORMATION_MESSAGE);
    }

    private void showAbout() {
        String aboutText = "Công cụ Migration Quyền Drive\n\n" +
                "Phiên bản nâng cao với tích hợp Google Apps Script\n" +
                "Version: 1.0\n\n" +
                "Tính năng:\n" +
                "• Xác thực Service Account\n" +
                "• Xử lý đa luồng\n" +
                "• Theo dõi tiến trình thời gian thực\n" +
                "• Tích hợp Google Sheets\n" +
                "• Ghi log chi tiết\n\n" +
                "© 2024 Drive Migration Tool";

        JOptionPane.showMessageDialog(this, aboutText, "Giới thiệu", JOptionPane.INFORMATION_MESSAGE);
    }

    private void saveLogs() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setSelectedFile(new File("migration-logs.txt"));

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            try (FileWriter writer = new FileWriter(fileChooser.getSelectedFile())) {
                writer.write(logArea.getText());
                appendLog("✓ Logs saved to: " + fileChooser.getSelectedFile().getName());
            } catch (IOException e) {
                appendLog("✗ Error saving logs: " + e.getMessage());
            }
        }
    }

    private void saveConfiguration() {
        Properties props = new Properties();
        props.setProperty("serviceAccountJsonPath", serviceAccountJsonPath);
        props.setProperty("serviceAccountEmail", serviceAccountEmailField.getText());
        props.setProperty("spreadsheetId", spreadsheetIdField.getText());

        try (FileOutputStream out = new FileOutputStream("drive-migration-config.properties")) {
            props.store(out, "Drive Migration Tool Configuration");
            appendLog("✓ Configuration saved");
        } catch (IOException e) {
            appendLog("✗ Error saving configuration: " + e.getMessage());
        }
    }

    private void loadConfiguration() {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("drive-migration-config.properties")) {
            props.load(in);
            serviceAccountJsonPath = props.getProperty("serviceAccountJsonPath", "");
            serviceAccountEmailField.setText(props.getProperty("serviceAccountEmail", ""));
            spreadsheetIdField.setText(props.getProperty("spreadsheetId", ""));
            appendLog("✓ Configuration loaded");
        } catch (IOException e) {
            appendLog("ℹ No previous configuration found");
        }
    }

    // Table Model
    private class UserTableModel extends AbstractTableModel {
        private final String[] columns = {
                "Email", "Status", "Progress", "Total Files", "Success", "Failed", "Restricted"
        };
        private final List<UserRecord> users = new ArrayList<>();

        @Override public int getRowCount() { return users.size(); }
        @Override public int getColumnCount() { return columns.length; }
        @Override public String getColumnName(int column) { return columns[column]; }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            switch (columnIndex) {
                case 0: case 1: return String.class;
                case 2: return JProgressBar.class;
                case 3: case 4: case 5: case 6: return Integer.class;
                default: return Object.class;
            }
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            UserRecord user = users.get(rowIndex);
            switch (columnIndex) {
                case 0: return user.email;
                case 1: return user.status;
                case 2: return user.progressBar;
                case 3: return user.totalFiles;
                case 4: return user.successFiles;
                case 5: return user.failedFiles;
                case 6: return user.restrictedFiles;
                default: return null;
            }
        }

        public void addUser(UserRecord user) {
            users.add(user);
            fireTableRowsInserted(users.size() - 1, users.size() - 1);
        }

        public UserRecord getUser(int index) {
            return (index >= 0 && index < users.size()) ? users.get(index) : null;
        }

        public void clearUsers() {
            int size = users.size();
            users.clear();
            if (size > 0) fireTableRowsDeleted(0, size - 1);
        }
    }

    // User Record class
    public static class UserRecord {
        public String email;
        public String status = "Not Started";
        public JProgressBar progressBar;
        public int totalFiles = 0;
        public int successFiles = 0;
        public int failedFiles = 0;
        public int restrictedFiles = 0;
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public int rowIndex;

        public UserRecord(String email) {
            this.email = email;
            this.progressBar = new JProgressBar(0, 100);
            this.progressBar.setStringPainted(true);
            this.progressBar.setString("0%");
        }
    }

    // Custom cell renderers
    private class StatusCellRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

            if (!isSelected) {
                String status = (String) value;
                switch (status) {
                    case "Completed": c.setBackground(new Color(200, 255, 200)); break;
                    case "In Progress": c.setBackground(new Color(200, 200, 255)); break;
                    case "Failed": c.setBackground(new Color(255, 200, 200)); break;
                    default: c.setBackground(Color.WHITE);
                }
            }
            return c;
        }
    }

    private class NumberCellRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            setHorizontalAlignment(JLabel.RIGHT);
            return c;
        }
    }

    private class ProgressBarRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus, int row, int column) {
            return (JProgressBar) value;
        }
    }

    // Main method
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            new DriveMigrationToolComplete().setVisible(true);
        });
    }
}