package ThuHoiEmail;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.directory.Directory;
import com.google.api.services.directory.DirectoryScopes;
import com.google.api.services.directory.model.User;
import com.google.api.services.directory.model.Users;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GmailBulkDeleteGUI extends JFrame {

    // UI Components
    private JTextField jsonFileField;
    private JTextField adminEmailField;
    private JComboBox<String> deleteModeCombo;
    private JTextField messageIdField;
    private JFormattedTextField dateFromField;
    private JFormattedTextField dateToField;
    private JTextField subjectField;
    private JSpinner threadSpinner;
    private JTextArea logArea;
    private JProgressBar progressBar;
    private JButton startButton;
    private JButton stopButton;
    private JButton saveLogButton;
    private JLabel statusLabel;
    private JLabel totalDeletedLabel;
    private JLabel currentUserLabel;
    private JLabel totalUsersLabel;
    private JLabel estimatedTimeLabel;

    // Dynamic panels
    private JPanel messageIdPanel;
    private JPanel dateRangePanel;
    private JPanel subjectPanel;

    // Config
    private File serviceAccountFile;
    private ExecutorService executor;
    private volatile boolean isRunning = false;
    private final AtomicInteger totalDeleted = new AtomicInteger(0);
    private final AtomicInteger processedUsers = new AtomicInteger(0);
    private long startTime;

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static HttpTransport httpTransport;

    // Rate limiting constants
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private static final long RATE_LIMIT_DELAY_MS = 1000;

    // Colors - Material Design Enhanced
    private static final Color PRIMARY_COLOR = new Color(33, 150, 243);
    private static final Color SUCCESS_COLOR = new Color(76, 175, 80);
    private static final Color ERROR_COLOR = new Color(244, 67, 54);
    private static final Color WARNING_COLOR = new Color(255, 152, 0);
    private static final Color INFO_COLOR = new Color(156, 39, 176);
    private static final Color BG_COLOR = new Color(245, 247, 250);
    private static final Color CARD_COLOR = Color.WHITE;
    private static final Color BORDER_COLOR = new Color(224, 224, 224);

    // Delete modes
    private static final String MODE_BY_DATE = "Xóa theo khoảng thời gian";
    private static final String MODE_BY_ID = "Xóa theo Message ID";
    private static final String MODE_BY_SUBJECT = "Xóa theo tiêu đề";
    private static final String MODE_BY_DATE_AND_SUBJECT = "Xóa theo ngày + tiêu đề";

    public GmailBulkDeleteGUI() {
        setTitle("Gmail Bulk Delete Tool - Professional Edition v2.0");
        setSize(1300, 850);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
        getContentPane().setBackground(BG_COLOR);

        initComponents();

        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Exception e) {
            showError("Lỗi khởi tạo HTTP Transport: " + e.getMessage());
        }
    }

    private void initComponents() {
        JPanel mainPanel = new JPanel(new BorderLayout(15, 15));
        mainPanel.setBorder(new EmptyBorder(20, 20, 20, 20));
        mainPanel.setBackground(BG_COLOR);

        // === TOP PANEL: CẤU HÌNH ===
        JPanel configPanel = createStyledCard();
        configPanel.setLayout(new GridBagLayout());
        configPanel.setBorder(BorderFactory.createCompoundBorder(
                createTitledBorder("Cấu hình hệ thống", PRIMARY_COLOR),
                new EmptyBorder(15, 15, 15, 15)
        ));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(8, 8, 8, 8);

        int row = 0;

        // File JSON
        gbc.gridx = 0; gbc.gridy = row; gbc.weightx = 0;
        configPanel.add(createLabel("Service Account JSON:", "folder.png"), gbc);

        gbc.gridx = 1; gbc.weightx = 1;
        jsonFileField = new JTextField();
        jsonFileField.setEditable(false);
        jsonFileField.setPreferredSize(new Dimension(400, 35));
        jsonFileField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        jsonFileField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        configPanel.add(jsonFileField, gbc);

        gbc.gridx = 2; gbc.weightx = 0;
        JButton browseButton = createStyledButton("Chọn file", PRIMARY_COLOR, "folder");
        browseButton.addActionListener(e -> chooseJsonFile());
        configPanel.add(browseButton, gbc);

        // Admin Email
        row++;
        gbc.gridx = 0; gbc.gridy = row; gbc.weightx = 0;
        configPanel.add(createLabel("Admin Email:", "user.png"), gbc);

        gbc.gridx = 1; gbc.gridwidth = 2; gbc.weightx = 1;
        adminEmailField = new JTextField("admin@elite-si.com");
        adminEmailField.setPreferredSize(new Dimension(400, 35));
        adminEmailField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        adminEmailField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        configPanel.add(adminEmailField, gbc);

        // Delete Mode Selector
        row++;
        gbc.gridx = 0; gbc.gridy = row; gbc.gridwidth = 1; gbc.weightx = 0;
        configPanel.add(createLabel("Chế độ xóa:", "settings.png"), gbc);

        gbc.gridx = 1; gbc.gridwidth = 2; gbc.weightx = 1;
        deleteModeCombo = new JComboBox<>(new String[]{
                MODE_BY_DATE,
                MODE_BY_ID,
                MODE_BY_SUBJECT,
                MODE_BY_DATE_AND_SUBJECT
        });
        deleteModeCombo.setPreferredSize(new Dimension(400, 35));
        deleteModeCombo.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        deleteModeCombo.addActionListener(e -> updateDeleteModeUI());
        configPanel.add(deleteModeCombo, gbc);

        // === DYNAMIC PANELS ===
        row++;
        gbc.gridx = 0; gbc.gridy = row; gbc.gridwidth = 3; gbc.weightx = 1;

        JPanel dynamicContainer = new JPanel(new CardLayout());
        dynamicContainer.setOpaque(false);

        // Message ID Panel
        messageIdPanel = createMessageIdPanel();

        // Date Range Panel
        dateRangePanel = createDateRangePanel();

        // Subject Panel
        subjectPanel = createSubjectPanel();

        configPanel.add(dateRangePanel, gbc);

        // Threads
        row++;
        gbc.gridx = 0; gbc.gridy = row; gbc.gridwidth = 1; gbc.weightx = 0;
        configPanel.add(createLabel("Số luồng:", "cpu.png"), gbc);

        gbc.gridx = 1; gbc.weightx = 0;
        threadSpinner = new JSpinner(new SpinnerNumberModel(5, 1, 20, 1));
        threadSpinner.setPreferredSize(new Dimension(100, 35));
        threadSpinner.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        ((JSpinner.DefaultEditor) threadSpinner.getEditor()).getTextField().setBorder(
                BorderFactory.createCompoundBorder(
                        new LineBorder(BORDER_COLOR, 1, true),
                        new EmptyBorder(5, 10, 5, 10)
                )
        );
        configPanel.add(threadSpinner, gbc);

        // === STATUS PANEL ===
        JPanel statusPanel = createStyledCard();
        statusPanel.setLayout(new GridLayout(5, 1, 10, 10));
        statusPanel.setBorder(BorderFactory.createCompoundBorder(
                createTitledBorder("Trạng thái hoạt động", SUCCESS_COLOR),
                new EmptyBorder(15, 15, 15, 15)
        ));

        statusLabel = createStatusLabel("Sẵn sàng", Color.DARK_GRAY, "clock");
        totalDeletedLabel = createStatusLabel("Đã xóa: 0 email", PRIMARY_COLOR, "mail");
        totalUsersLabel = createStatusLabel("Users: 0/0", PRIMARY_COLOR, "users");
        currentUserLabel = createStatusLabel("Đang xử lý: -", Color.DARK_GRAY, "user");
        estimatedTimeLabel = createStatusLabel("Thời gian: -", Color.DARK_GRAY, "timer");

        statusPanel.add(statusLabel);
        statusPanel.add(totalDeletedLabel);
        statusPanel.add(totalUsersLabel);
        statusPanel.add(currentUserLabel);
        statusPanel.add(estimatedTimeLabel);

        // === LOG PANEL ===
        JPanel logPanel = createStyledCard();
        logPanel.setLayout(new BorderLayout(10, 10));
        logPanel.setBorder(BorderFactory.createCompoundBorder(
                createTitledBorder("Nhật ký hoạt động", WARNING_COLOR),
                new EmptyBorder(15, 15, 15, 15)
        ));

        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font("Consolas", Font.PLAIN, 11));
        logArea.setLineWrap(false);
        logArea.setBackground(new Color(250, 250, 250));
        logArea.setBorder(new EmptyBorder(10, 10, 10, 10));
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setPreferredSize(new Dimension(1200, 320));
        scrollPane.setBorder(new LineBorder(BORDER_COLOR, 1, true));
        logPanel.add(scrollPane, BorderLayout.CENTER);

        // === BUTTON PANEL ===
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 15, 10));
        buttonPanel.setOpaque(false);

        startButton = createStyledButton("Bắt đầu xóa", SUCCESS_COLOR, "play");
        startButton.setPreferredSize(new Dimension(180, 50));
        startButton.setFont(new Font("Segoe UI", Font.BOLD, 14));
        startButton.addActionListener(e -> startDeleting());

        stopButton = createStyledButton("Dừng lại", ERROR_COLOR, "stop");
        stopButton.setPreferredSize(new Dimension(180, 50));
        stopButton.setFont(new Font("Segoe UI", Font.BOLD, 14));
        stopButton.setEnabled(false);
        stopButton.addActionListener(e -> stopDeleting());

        JButton clearButton = createStyledButton("Xóa log", new Color(158, 158, 158), "trash");
        clearButton.setPreferredSize(new Dimension(140, 50));
        clearButton.addActionListener(e -> logArea.setText(""));

        saveLogButton = createStyledButton("Lưu log", INFO_COLOR, "save");
        saveLogButton.setPreferredSize(new Dimension(140, 50));
        saveLogButton.addActionListener(e -> saveLog());

        buttonPanel.add(startButton);
        buttonPanel.add(stopButton);
        buttonPanel.add(clearButton);
        buttonPanel.add(saveLogButton);

        // === PROGRESS BAR ===
        progressBar = new JProgressBar();
        progressBar.setStringPainted(true);
        progressBar.setPreferredSize(new Dimension(1200, 35));
        progressBar.setForeground(SUCCESS_COLOR);
        progressBar.setFont(new Font("Segoe UI", Font.BOLD, 12));
        progressBar.setBorder(new LineBorder(BORDER_COLOR, 1, true));

        // === ASSEMBLY ===
        JPanel topSection = new JPanel(new GridLayout(1, 2, 15, 0));
        topSection.setOpaque(false);
        topSection.add(configPanel);
        topSection.add(statusPanel);

        JPanel bottomSection = new JPanel(new BorderLayout(10, 10));
        bottomSection.setOpaque(false);
        bottomSection.add(buttonPanel, BorderLayout.NORTH);
        bottomSection.add(progressBar, BorderLayout.CENTER);

        mainPanel.add(topSection, BorderLayout.NORTH);
        mainPanel.add(logPanel, BorderLayout.CENTER);
        mainPanel.add(bottomSection, BorderLayout.SOUTH);

        add(mainPanel);
    }

    private JPanel createMessageIdPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(INFO_COLOR, 1, true),
                new EmptyBorder(10, 10, 10, 10)
        ));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);

        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0;
        panel.add(createLabel("Message ID:", "mail.png"), gbc);

        gbc.gridx = 1; gbc.weightx = 1;
        messageIdField = new JTextField();
        messageIdField.setPreferredSize(new Dimension(400, 35));
        messageIdField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        messageIdField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        messageIdField.setToolTipText("Nhập Message ID cần xóa");
        panel.add(messageIdField, gbc);

        return panel;
    }

    private JPanel createDateRangePanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(SUCCESS_COLOR, 1, true),
                new EmptyBorder(10, 10, 10, 10)
        ));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);

        // From Date
        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0;
        panel.add(createLabel("Từ ngày:", "calendar.png"), gbc);

        gbc.gridx = 1; gbc.weightx = 0.5;
        JPanel fromPanel = new JPanel(new BorderLayout(5, 0));
        fromPanel.setOpaque(false);
        dateFromField = new JFormattedTextField(createDateFormatter());
        dateFromField.setPreferredSize(new Dimension(150, 35));
        dateFromField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        dateFromField.setText(new SimpleDateFormat("dd/MM/yyyy").format(new Date()));
        dateFromField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        fromPanel.add(dateFromField, BorderLayout.CENTER);

        JButton fromCalendarBtn = createIconButton("📅");
        fromCalendarBtn.addActionListener(e -> showDatePicker(dateFromField));
        fromPanel.add(fromCalendarBtn, BorderLayout.EAST);
        panel.add(fromPanel, gbc);

        // To Date
        gbc.gridx = 2; gbc.weightx = 0;
        panel.add(createLabel("Đến ngày:", "calendar.png"), gbc);

        gbc.gridx = 3; gbc.weightx = 0.5;
        JPanel toPanel = new JPanel(new BorderLayout(5, 0));
        toPanel.setOpaque(false);
        dateToField = new JFormattedTextField(createDateFormatter());
        dateToField.setPreferredSize(new Dimension(150, 35));
        dateToField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, 1);
        dateToField.setText(new SimpleDateFormat("dd/MM/yyyy").format(cal.getTime()));
        dateToField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        toPanel.add(dateToField, BorderLayout.CENTER);

        JButton toCalendarBtn = createIconButton("📅");
        toCalendarBtn.addActionListener(e -> showDatePicker(dateToField));
        toPanel.add(toCalendarBtn, BorderLayout.EAST);
        panel.add(toPanel, gbc);

        return panel;
    }

    private JPanel createSubjectPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(WARNING_COLOR, 1, true),
                new EmptyBorder(10, 10, 10, 10)
        ));

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);

        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0;
        panel.add(createLabel("Tiêu đề email:", "mail.png"), gbc);

        gbc.gridx = 1; gbc.weightx = 1;
        subjectField = new JTextField();
        subjectField.setPreferredSize(new Dimension(400, 35));
        subjectField.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        subjectField.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 10, 5, 10)
        ));
        subjectField.setToolTipText("Nhập tiêu đề email cần tìm và xóa");
        panel.add(subjectField, gbc);

        return panel;
    }

    private void updateDeleteModeUI() {
        String selectedMode = (String) deleteModeCombo.getSelectedItem();

        // Remove all dynamic panels first
        Container parent = dateRangePanel.getParent();
        if (parent != null) {
            parent.remove(messageIdPanel);
            parent.remove(dateRangePanel);
            parent.remove(subjectPanel);

            GridBagConstraints gbc = new GridBagConstraints();
            gbc.fill = GridBagConstraints.HORIZONTAL;
            gbc.gridx = 0;
            gbc.gridy = 3;
            gbc.gridwidth = 3;
            gbc.weightx = 1;
            gbc.insets = new Insets(8, 8, 8, 8);

            // Add appropriate panels based on mode
            if (MODE_BY_ID.equals(selectedMode)) {
                parent.add(messageIdPanel, gbc);
            } else if (MODE_BY_DATE.equals(selectedMode)) {
                parent.add(dateRangePanel, gbc);
            } else if (MODE_BY_SUBJECT.equals(selectedMode)) {
                parent.add(subjectPanel, gbc);
            } else if (MODE_BY_DATE_AND_SUBJECT.equals(selectedMode)) {
                gbc.gridy = 3;
                parent.add(dateRangePanel, gbc);
                gbc.gridy = 4;
                parent.add(subjectPanel, gbc);
            }

            parent.revalidate();
            parent.repaint();
        }
    }

    private TitledBorder createTitledBorder(String title, Color color) {
        return new TitledBorder(
                new LineBorder(color, 2, true),
                title,
                TitledBorder.LEFT,
                TitledBorder.TOP,
                new Font("Segoe UI", Font.BOLD, 14),
                color
        );
    }

    private JLabel createLabel(String text, String icon) {
        JLabel label = new JLabel(text);
        label.setFont(new Font("Segoe UI", Font.BOLD, 12));
        label.setForeground(new Color(60, 60, 60));
        return label;
    }

    private JPanel createStyledCard() {
        JPanel panel = new JPanel();
        panel.setBackground(CARD_COLOR);
        panel.setBorder(BorderFactory.createCompoundBorder(
                new LineBorder(BORDER_COLOR, 1, true),
                new EmptyBorder(5, 5, 5, 5)
        ));
        return panel;
    }

    private JButton createStyledButton(String text, Color color, String icon) {
        JButton button = new JButton(text);
        button.setBackground(color);
        button.setForeground(Color.WHITE);
        button.setFocusPainted(false);
        button.setBorderPainted(false);
        button.setFont(new Font("Segoe UI", Font.BOLD, 12));
        button.setCursor(new Cursor(Cursor.HAND_CURSOR));
        button.setBorder(new EmptyBorder(10, 20, 10, 20));

        button.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                button.setBackground(color.brighter());
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                button.setBackground(color);
            }
        });

        return button;
    }

    private JButton createIconButton(String icon) {
        JButton button = new JButton(icon);
        button.setPreferredSize(new Dimension(45, 35));
        button.setBackground(PRIMARY_COLOR);
        button.setForeground(Color.WHITE);
        button.setFocusPainted(false);
        button.setBorderPainted(false);
        button.setCursor(new Cursor(Cursor.HAND_CURSOR));
        button.setFont(new Font("Segoe UI Emoji", Font.PLAIN, 14));
        return button;
    }

    private JLabel createStatusLabel(String text, Color color, String icon) {
        JLabel label = new JLabel(text);
        label.setFont(new Font("Segoe UI", Font.BOLD, 13));
        label.setForeground(color);
        label.setBorder(new EmptyBorder(5, 5, 5, 5));
        return label;
    }

    private JFormattedTextField.AbstractFormatter createDateFormatter() {
        return new JFormattedTextField.AbstractFormatter() {
            private SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

            @Override
            public Object stringToValue(String text) throws java.text.ParseException {
                return sdf.parse(text);
            }

            @Override
            public String valueToString(Object value) throws java.text.ParseException {
                if (value == null) return "";
                return sdf.format((Date) value);
            }
        };
    }

    private void showDatePicker(JFormattedTextField field) {
        JDialog dialog = new JDialog(this, "Chọn ngày", true);
        dialog.setLayout(new BorderLayout(10, 10));
        dialog.getContentPane().setBackground(CARD_COLOR);

        Calendar cal = Calendar.getInstance();
        try {
            Date currentDate = new SimpleDateFormat("dd/MM/yyyy").parse(field.getText());
            cal.setTime(currentDate);
        } catch (Exception e) {
            // Use current date if parsing fails
        }

        final int[] currentMonth = {cal.get(Calendar.MONTH)};
        final int[] currentYear = {cal.get(Calendar.YEAR)};

        // Header Panel with navigation
        JPanel headerPanel = new JPanel(new BorderLayout());
        headerPanel.setBackground(PRIMARY_COLOR);
        headerPanel.setBorder(new EmptyBorder(15, 15, 15, 15));

        JButton prevButton = new JButton("◄");
        styleNavButton(prevButton);

        JLabel monthYearLabel = new JLabel(getMonthYearText(currentMonth[0], currentYear[0]), SwingConstants.CENTER);
        monthYearLabel.setFont(new Font("Segoe UI", Font.BOLD, 16));
        monthYearLabel.setForeground(Color.WHITE);

        JButton nextButton = new JButton("►");
        styleNavButton(nextButton);

        headerPanel.add(prevButton, BorderLayout.WEST);
        headerPanel.add(monthYearLabel, BorderLayout.CENTER);
        headerPanel.add(nextButton, BorderLayout.EAST);

        // Calendar Panel
        JPanel calendarPanel = new JPanel(new GridLayout(7, 7, 5, 5));
        calendarPanel.setBackground(CARD_COLOR);
        calendarPanel.setBorder(new EmptyBorder(15, 15, 15, 15));

        Runnable updateCalendar = () -> {
            calendarPanel.removeAll();

            // Day headers
            String[] dayNames = {"CN", "T2", "T3", "T4", "T5", "T6", "T7"};
            for (String day : dayNames) {
                JLabel dayLabel = new JLabel(day, SwingConstants.CENTER);
                dayLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
                dayLabel.setForeground(PRIMARY_COLOR);
                dayLabel.setPreferredSize(new Dimension(50, 35));
                calendarPanel.add(dayLabel);
            }

            // Calculate calendar
            Calendar tempCal = Calendar.getInstance();
            tempCal.set(currentYear[0], currentMonth[0], 1);
            int firstDayOfWeek = tempCal.get(Calendar.DAY_OF_WEEK) - 1;
            int daysInMonth = tempCal.getActualMaximum(Calendar.DAY_OF_MONTH);

            // Empty cells before first day
            for (int i = 0; i < firstDayOfWeek; i++) {
                calendarPanel.add(new JLabel(""));
            }

            // Day buttons
            for (int day = 1; day <= daysInMonth; day++) {
                final int selectedDay = day;
                JButton dayButton = new JButton(String.valueOf(day));
                dayButton.setPreferredSize(new Dimension(50, 35));
                dayButton.setFont(new Font("Segoe UI", Font.PLAIN, 12));
                dayButton.setBackground(Color.WHITE);
                dayButton.setForeground(Color.DARK_GRAY);
                dayButton.setBorder(new LineBorder(BORDER_COLOR, 1, true));
                dayButton.setFocusPainted(false);
                dayButton.setCursor(new Cursor(Cursor.HAND_CURSOR));

                dayButton.addMouseListener(new java.awt.event.MouseAdapter() {
                    public void mouseEntered(java.awt.event.MouseEvent evt) {
                        dayButton.setBackground(PRIMARY_COLOR);
                        dayButton.setForeground(Color.WHITE);
                    }
                    public void mouseExited(java.awt.event.MouseEvent evt) {
                        dayButton.setBackground(Color.WHITE);
                        dayButton.setForeground(Color.DARK_GRAY);
                    }
                });

                dayButton.addActionListener(e -> {
                    String dateStr = String.format("%02d/%02d/%d", selectedDay, currentMonth[0] + 1, currentYear[0]);
                    field.setText(dateStr);
                    dialog.dispose();
                });
                calendarPanel.add(dayButton);
            }

            monthYearLabel.setText(getMonthYearText(currentMonth[0], currentYear[0]));
            calendarPanel.revalidate();
            calendarPanel.repaint();
        };

        prevButton.addActionListener(e -> {
            currentMonth[0]--;
            if (currentMonth[0] < 0) {
                currentMonth[0] = 11;
                currentYear[0]--;
            }
            updateCalendar.run();
        });

        nextButton.addActionListener(e -> {
            currentMonth[0]++;
            if (currentMonth[0] > 11) {
                currentMonth[0] = 0;
                currentYear[0]++;
            }
            updateCalendar.run();
        });

        updateCalendar.run();

        // Today button
        JPanel footerPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        footerPanel.setBackground(CARD_COLOR);
        JButton todayButton = createStyledButton("Hôm nay", SUCCESS_COLOR, "today");
        todayButton.addActionListener(e -> {
            field.setText(new SimpleDateFormat("dd/MM/yyyy").format(new Date()));
            dialog.dispose();
        });
        footerPanel.add(todayButton);

        dialog.add(headerPanel, BorderLayout.NORTH);
        dialog.add(calendarPanel, BorderLayout.CENTER);
        dialog.add(footerPanel, BorderLayout.SOUTH);
        dialog.pack();
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private String getMonthYearText(int month, int year) {
        String[] monthNames = {"Tháng 1", "Tháng 2", "Tháng 3", "Tháng 4", "Tháng 5", "Tháng 6",
                "Tháng 7", "Tháng 8", "Tháng 9", "Tháng 10", "Tháng 11", "Tháng 12"};
        return monthNames[month] + " " + year;
    }

    private void styleNavButton(JButton button) {
        button.setFont(new Font("Segoe UI", Font.BOLD, 16));
        button.setForeground(Color.WHITE);
        button.setBackground(PRIMARY_COLOR);
        button.setBorderPainted(false);
        button.setFocusPainted(false);
        button.setCursor(new Cursor(Cursor.HAND_CURSOR));
        button.setPreferredSize(new Dimension(50, 40));
    }

    private boolean validateDates() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            Date fromDate = sdf.parse(dateFromField.getText());
            Date toDate = sdf.parse(dateToField.getText());

            if (fromDate.after(toDate)) {
                showError("Ngày bắt đầu không được sau ngày kết thúc!");
                return false;
            }

            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.YEAR, -5);
            if (fromDate.before(cal.getTime())) {
                int result = JOptionPane.showConfirmDialog(this,
                        "Ngày bắt đầu cách đây hơn 5 năm. Bạn có chắc chắn muốn tiếp tục?",
                        "Xác nhận", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
                return result == JOptionPane.YES_OPTION;
            }

            return true;
        } catch (Exception e) {
            showError("Định dạng ngày không hợp lệ! Vui lòng dùng dd/MM/yyyy");
            return false;
        }
    }

    private void chooseJsonFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("JSON files", "json"));

        if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            serviceAccountFile = chooser.getSelectedFile();
            jsonFileField.setText(serviceAccountFile.getAbsolutePath());
            detailedLog("✓ Đã chọn file: " + serviceAccountFile.getName(), "INFO");
        }
    }

    private void startDeleting() {
        // Validate
        if (serviceAccountFile == null || !serviceAccountFile.exists()) {
            showError("Vui lòng chọn file Service Account JSON!");
            return;
        }

        if (adminEmailField.getText().trim().isEmpty()) {
            showError("Vui lòng nhập Admin Email!");
            return;
        }

        String mode = (String) deleteModeCombo.getSelectedItem();

        // Validate based on mode
        if (MODE_BY_ID.equals(mode)) {
            if (messageIdField.getText().trim().isEmpty()) {
                showError("Vui lòng nhập Message ID!");
                return;
            }
        } else if (MODE_BY_DATE.equals(mode)) {
            if (!validateDates()) {
                return;
            }
        } else if (MODE_BY_SUBJECT.equals(mode)) {
            if (subjectField.getText().trim().isEmpty()) {
                showError("Vui lòng nhập tiêu đề email!");
                return;
            }
        } else if (MODE_BY_DATE_AND_SUBJECT.equals(mode)) {
            boolean hasDate = !dateFromField.getText().trim().isEmpty() && !dateToField.getText().trim().isEmpty();
            boolean hasSubject = !subjectField.getText().trim().isEmpty();

            if (!hasDate && !hasSubject) {
                showError("Vui lòng nhập ít nhất ngày hoặc tiêu đề!");
                return;
            }

            if (hasDate && !validateDates()) {
                return;
            }
        }

        // Confirm
        int confirm = JOptionPane.showConfirmDialog(this,
                "Bạn có chắc chắn muốn xóa email?\nHành động này KHÔNG THỂ hoàn tác!",
                "Xác nhận xóa", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);

        if (confirm != JOptionPane.YES_OPTION) {
            return;
        }

        // Disable buttons
        startButton.setEnabled(false);
        stopButton.setEnabled(true);
        isRunning = true;
        totalDeleted.set(0);
        processedUsers.set(0);
        startTime = System.currentTimeMillis();

        // Get config
        String adminEmail = adminEmailField.getText().trim();
        int threadCount = (Integer) threadSpinner.getValue();

        detailedLog("\n" + "═".repeat(80), "SYSTEM");
        detailedLog("BẮT ĐẦU TIẾN TRÌNH XÓA EMAIL", "SYSTEM");
        detailedLog("═".repeat(80), "SYSTEM");
        detailedLog("Cấu hình:", "INFO");
        detailedLog("  • Admin Email: " + adminEmail, "INFO");
        detailedLog("  • Chế độ: " + mode, "INFO");

        if (MODE_BY_ID.equals(mode)) {
            detailedLog("  • Message ID: " + messageIdField.getText().trim(), "INFO");
        } else if (MODE_BY_DATE.equals(mode)) {
            detailedLog("  • Khoảng thời gian: " + dateFromField.getText() + " → " + dateToField.getText(), "INFO");
        } else if (MODE_BY_SUBJECT.equals(mode)) {
            detailedLog("  • Tiêu đề: " + subjectField.getText().trim(), "INFO");
        } else if (MODE_BY_DATE_AND_SUBJECT.equals(mode)) {
            if (!dateFromField.getText().trim().isEmpty()) {
                detailedLog("  • Khoảng thời gian: " + dateFromField.getText() + " → " + dateToField.getText(), "INFO");
            }
            if (!subjectField.getText().trim().isEmpty()) {
                detailedLog("  • Tiêu đề: " + subjectField.getText().trim(), "INFO");
            }
        }

        detailedLog("  • Số luồng xử lý: " + threadCount, "INFO");
        detailedLog("  • Thời gian bắt đầu: " + getCurrentTimestamp(), "INFO");
        detailedLog("═".repeat(80) + "\n", "SYSTEM");

        statusLabel.setText("Đang chạy...");
        statusLabel.setForeground(WARNING_COLOR);

        // Run in background thread
        new Thread(() -> {
            try {
                executeDelete(adminEmail, mode, threadCount);
            } catch (Exception e) {
                detailedLog("✗ LỖI NGHIÊM TRỌNG: " + e.getMessage(), "ERROR");
                e.printStackTrace();
            } finally {
                SwingUtilities.invokeLater(() -> {
                    startButton.setEnabled(true);
                    stopButton.setEnabled(false);
                    isRunning = false;
                    statusLabel.setText("Hoàn tất");
                    statusLabel.setForeground(SUCCESS_COLOR);
                });
            }
        }).start();
    }

    private void stopDeleting() {
        if (executor != null && !executor.isShutdown()) {
            detailedLog("\n■ ĐANG DỪNG TIẾN TRÌNH...", "WARNING");
            executor.shutdownNow();
            isRunning = false;
            statusLabel.setText("Đã dừng");
            statusLabel.setForeground(ERROR_COLOR);
            detailedLog("✓ Đã dừng thành công", "WARNING");
        }
    }

    private void executeDelete(String adminEmail, String mode, int threadCount) throws Exception {
        // Get users
        detailedLog("Đang lấy danh sách user từ Google Workspace...", "INFO");
        List<String> users = getAllUsers(adminEmail);
        detailedLog("✓ Đã lấy được " + users.size() + " user", "SUCCESS");

        SwingUtilities.invokeLater(() -> {
            totalUsersLabel.setText("Users: 0/" + users.size());
        });

        progressBar.setMaximum(users.size());
        progressBar.setValue(0);

        // Create thread pool
        executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        detailedLog("\nBắt đầu xử lý " + users.size() + " user với " + threadCount + " luồng...\n", "INFO");

        // Process each user
        for (String userEmail : users) {
            if (!isRunning) break;

            Future<?> future = executor.submit(() -> {
                if (!isRunning) return;
                processUserByMode(userEmail, mode);
                int processed = processedUsers.incrementAndGet();
                SwingUtilities.invokeLater(() -> {
                    progressBar.setValue(processed);
                    totalUsersLabel.setText("Users: " + processed + "/" + users.size());
                    updateEstimatedTime(processed, users.size());
                });
            });
            futures.add(future);
        }

        // Wait for completion
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                detailedLog("✗ Lỗi task: " + e.getMessage(), "ERROR");
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long duration = System.currentTimeMillis() - startTime;
        String durationStr = formatDuration(duration);

        detailedLog("\n" + "═".repeat(80), "SYSTEM");
        detailedLog("HOÀN TẤT TIẾN TRÌNH XÓA EMAIL", "SUCCESS");
        detailedLog("═".repeat(80), "SYSTEM");
        detailedLog("Thống kê:", "INFO");
        detailedLog("  • Tổng số email đã xóa: " + totalDeleted.get(), "SUCCESS");
        detailedLog("  • Số user đã xử lý: " + processedUsers.get() + "/" + users.size(), "SUCCESS");
        detailedLog("  • Thời gian thực hiện: " + durationStr, "INFO");
        detailedLog("  • Thời gian kết thúc: " + getCurrentTimestamp(), "INFO");
        detailedLog("═".repeat(80) + "\n", "SYSTEM");
    }

    private void processUserByMode(String userEmail, String mode) {
        SwingUtilities.invokeLater(() -> {
            currentUserLabel.setText("Đang xử lý: " + userEmail);
            currentUserLabel.setForeground(WARNING_COLOR);
        });

        detailedLog("\n┌─ [" + Thread.currentThread().getName() + "] " + userEmail, "USER");

        try {
            Gmail service = createGmailService(userEmail);
            String query = buildQuery(mode);

            if (query == null) {
                detailedLog("│  ✗ Không thể tạo query", "ERROR");
                detailedLog("└─ Kết thúc xử lý " + userEmail + "\n", "USER");
                return;
            }

            detailedLog("│  Query: " + query, "INFO");

            int userDeleted = 0;
            String pageToken = null;
            int pageNum = 1;
            int totalEmails = 0;

            do {
                if (!isRunning) break;

                ListMessagesResponse response = null;
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    try {
                        response = service.users().messages()
                                .list(userEmail)
                                .setQ(query)
                                .setMaxResults(500L)
                                .setPageToken(pageToken)
                                .execute();
                        break;
                    } catch (GoogleJsonResponseException e) {
                        if (e.getStatusCode() == 429 || e.getStatusCode() == 503) {
                            detailedLog("│  ⚠ Rate limit (HTTP " + e.getStatusCode() + "), retry " + (retry + 1) + "/" + MAX_RETRIES, "WARNING");
                            Thread.sleep(RETRY_DELAY_MS * (retry + 1));
                        } else {
                            throw e;
                        }
                    } catch (IOException e) {
                        detailedLog("│  ⚠ Network error, retry " + (retry + 1) + "/" + MAX_RETRIES, "WARNING");
                        Thread.sleep(RETRY_DELAY_MS);
                    }
                }

                if (response == null) {
                    detailedLog("│  ✗ Không thể lấy danh sách email sau " + MAX_RETRIES + " lần thử", "ERROR");
                    break;
                }

                List<Message> messages = response.getMessages();

                if (messages == null || messages.isEmpty()) {
                    if (pageNum == 1) {
                        detailedLog("│  Không có email nào phù hợp", "INFO");
                    }
                    break;
                }

                totalEmails += messages.size();
                detailedLog("│  Trang " + pageNum + ": Tìm thấy " + messages.size() + " email", "INFO");

                int pageDeleted = 0;
                for (Message msg : messages) {
                    if (!isRunning) break;

                    boolean deleted = false;
                    for (int retry = 0; retry < MAX_RETRIES; retry++) {
                        try {
                            service.users().messages().delete(userEmail, msg.getId()).execute();
                            deleted = true;
                            break;
                        } catch (GoogleJsonResponseException e) {
                            if (e.getStatusCode() == 429 || e.getStatusCode() == 503) {
                                Thread.sleep(RETRY_DELAY_MS * (retry + 1));
                            } else if (e.getStatusCode() == 404) {
                                deleted = true;
                                break;
                            } else {
                                break;
                            }
                        } catch (IOException e) {
                            if (retry < MAX_RETRIES - 1) {
                                Thread.sleep(RETRY_DELAY_MS);
                            }
                        }
                    }

                    if (deleted) {
                        pageDeleted++;
                        userDeleted++;
                        int total = totalDeleted.incrementAndGet();
                        SwingUtilities.invokeLater(() ->
                                totalDeletedLabel.setText("Đã xóa: " + total + " email")
                        );
                    }
                }

                detailedLog("│  ✓ Đã xóa " + pageDeleted + "/" + messages.size() + " email ở trang " + pageNum, "SUCCESS");

                pageToken = response.getNextPageToken();
                pageNum++;
                Thread.sleep(RATE_LIMIT_DELAY_MS);

            } while (pageToken != null && isRunning);

            if (userDeleted > 0) {
                detailedLog("│  ✓ Hoàn tất: Đã xóa " + userDeleted + "/" + totalEmails + " email", "SUCCESS");
            } else {
                detailedLog("│  Không có email nào bị xóa", "INFO");
            }
            detailedLog("└─ Kết thúc xử lý " + userEmail + "\n", "USER");

        } catch (GoogleJsonResponseException e) {
            detailedLog("│  ✗ Lỗi Google API [HTTP " + e.getStatusCode() + "]: " + e.getStatusMessage(), "ERROR");
            detailedLog("└─ Lỗi khi xử lý " + userEmail + "\n", "ERROR");
        } catch (Exception e) {
            detailedLog("│  ✗ Lỗi: " + e.getClass().getSimpleName() + " - " + e.getMessage(), "ERROR");
            detailedLog("└─ Lỗi khi xử lý " + userEmail + "\n", "ERROR");
            e.printStackTrace();
        }
    }

    private String buildQuery(String mode) {
        StringBuilder query = new StringBuilder();

        if (MODE_BY_ID.equals(mode)) {
            String messageId = messageIdField.getText().trim();
            query.append("rfc822msgid:").append(messageId);
        } else if (MODE_BY_DATE.equals(mode)) {
            String dateFrom = dateFromField.getText();
            String dateTo = dateToField.getText();
            query.append("after:").append(formatDateForGmail(dateFrom));
            query.append(" before:").append(formatDateForGmail(dateTo));
        } else if (MODE_BY_SUBJECT.equals(mode)) {
            String subject = subjectField.getText().trim();
            query.append("subject:\"").append(subject).append("\"");
        } else if (MODE_BY_DATE_AND_SUBJECT.equals(mode)) {
            boolean hasDate = !dateFromField.getText().trim().isEmpty() && !dateToField.getText().trim().isEmpty();
            boolean hasSubject = !subjectField.getText().trim().isEmpty();

            if (hasDate) {
                String dateFrom = dateFromField.getText();
                String dateTo = dateToField.getText();
                query.append("after:").append(formatDateForGmail(dateFrom));
                query.append(" before:").append(formatDateForGmail(dateTo));
            }

            if (hasSubject) {
                if (hasDate) query.append(" ");
                String subject = subjectField.getText().trim();
                query.append("subject:\"").append(subject).append("\"");
            }
        }

        return query.length() > 0 ? query.toString() : null;
    }

    private List<String> getAllUsers(String adminEmail) throws Exception {
        Directory service = createDirectoryService(adminEmail);
        List<String> allUsers = new ArrayList<>();
        String pageToken = null;
        int pageNum = 1;

        do {
            detailedLog("  Đang lấy trang " + pageNum + " danh sách user...", "INFO");

            Users result = null;
            for (int retry = 0; retry < MAX_RETRIES; retry++) {
                try {
                    result = service.users().list()
                            .setCustomer("my_customer")
                            .setMaxResults(100)
                            .setOrderBy("email")
                            .setPageToken(pageToken)
                            .execute();
                    break;
                } catch (GoogleJsonResponseException e) {
                    if (e.getStatusCode() == 429 || e.getStatusCode() == 503) {
                        detailedLog("  ⚠ Rate limit khi lấy users, retry " + (retry + 1) + "/" + MAX_RETRIES, "WARNING");
                        Thread.sleep(RETRY_DELAY_MS * (retry + 1));
                    } else {
                        throw e;
                    }
                } catch (IOException e) {
                    detailedLog("  ⚠ Network error khi lấy users, retry " + (retry + 1) + "/" + MAX_RETRIES, "WARNING");
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }

            if (result == null) {
                throw new Exception("Không thể lấy danh sách users sau " + MAX_RETRIES + " lần thử");
            }

            List<User> users = result.getUsers();
            if (users != null) {
                for (User user : users) {
                    allUsers.add(user.getPrimaryEmail());
                }
                detailedLog("  ✓ Trang " + pageNum + ": " + users.size() + " users", "SUCCESS");
            }
            pageToken = result.getNextPageToken();
            pageNum++;
        } while (pageToken != null);

        return allUsers;
    }

    private Gmail createGmailService(String userEmail) throws Exception {
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(serviceAccountFile))
                .createScoped(Arrays.asList(GmailScopes.MAIL_GOOGLE_COM))
                .createDelegated(userEmail);

        return new Gmail.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName("Gmail Bulk Delete")
                .build();
    }

    private Directory createDirectoryService(String adminEmail) throws Exception {
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(serviceAccountFile))
                .createScoped(Arrays.asList(DirectoryScopes.ADMIN_DIRECTORY_USER_READONLY))
                .createDelegated(adminEmail);

        return new Directory.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName("Gmail Bulk Delete")
                .build();
    }

    private String formatDateForGmail(String date) {
        try {
            SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MM/yyyy");
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy/MM/dd");
            Date d = inputFormat.parse(date);
            return outputFormat.format(d);
        } catch (Exception e) {
            return date;
        }
    }

    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
    }

    private String formatDuration(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) {
            return String.format("%d giờ %d phút %d giây", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%d phút %d giây", minutes, seconds % 60);
        } else {
            return seconds + " giây";
        }
    }

    private void updateEstimatedTime(int processed, int total) {
        if (processed == 0) return;

        long elapsed = System.currentTimeMillis() - startTime;
        long avgTimePerUser = elapsed / processed;
        long remaining = avgTimePerUser * (total - processed);

        String remainingStr = formatDuration(remaining);
        estimatedTimeLabel.setText("Thời gian còn lại: ~" + remainingStr);
    }

    private void detailedLog(String message, String level) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        String prefix = "";

        switch (level) {
            case "ERROR": prefix = "✗"; break;
            case "WARNING": prefix = "⚠"; break;
            case "SUCCESS": prefix = "✓"; break;
            case "INFO": prefix = "ℹ"; break;
            case "USER": prefix = "👤"; break;
            case "SYSTEM": prefix = "⚙"; break;
        }

        String logMessage;
        if (level.equals("SYSTEM") || message.startsWith("═") || message.startsWith("┌") || message.startsWith("└") || message.startsWith("│")) {
            logMessage = message;
        } else {
            logMessage = "[" + timestamp + "] " + prefix + " " + message;
        }

        SwingUtilities.invokeLater(() -> {
            logArea.append(logMessage + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });

        System.out.println(logMessage);
    }

    private void saveLog() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Lưu log");

        String defaultFileName = "gmail_delete_log_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt";
        fileChooser.setSelectedFile(new File(defaultFileName));

        fileChooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("Text files (*.txt)", "txt"));

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File file = fileChooser.getSelectedFile();

            if (!file.getName().toLowerCase().endsWith(".txt")) {
                file = new File(file.getAbsolutePath() + ".txt");
            }

            try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                writer.println("═══════════════════════════════════════════════════════════════");
                writer.println("GMAIL BULK DELETE TOOL - LOG REPORT");
                writer.println("═══════════════════════════════════════════════════════════════");
                writer.println("Generated: " + getCurrentTimestamp());
                writer.println("Admin Email: " + adminEmailField.getText());
                writer.println("Delete Mode: " + deleteModeCombo.getSelectedItem());
                writer.println("═══════════════════════════════════════════════════════════════\n");
                writer.println(logArea.getText());
                writer.println("\n═══════════════════════════════════════════════════════════════");
                writer.println("END OF REPORT");
                writer.println("═══════════════════════════════════════════════════════════════");

                detailedLog("Đã lưu log vào file: " + file.getAbsolutePath(), "SUCCESS");
                JOptionPane.showMessageDialog(this,
                        "✓ Đã lưu log thành công!\n" + file.getAbsolutePath(),
                        "Thành công", JOptionPane.INFORMATION_MESSAGE);

            } catch (IOException e) {
                showError("Lỗi khi lưu file: " + e.getMessage());
                detailedLog("Không thể lưu log: " + e.getMessage(), "ERROR");
            }
        }
    }

    private void showError(String message) {
        JOptionPane.showMessageDialog(this, message, "Lỗi", JOptionPane.ERROR_MESSAGE);
    }

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

            Font font = new Font("Segoe UI", Font.PLAIN, 12);
            java.util.Enumeration<Object> keys = UIManager.getDefaults().keys();
            while (keys.hasMoreElements()) {
                Object key = keys.nextElement();
                Object value = UIManager.get(key);
                if (value instanceof javax.swing.plaf.FontUIResource) {
                    UIManager.put(key, font);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        SwingUtilities.invokeLater(() -> {
            GmailBulkDeleteGUI gui = new GmailBulkDeleteGUI();
            gui.setVisible(true);
        });
    }
}