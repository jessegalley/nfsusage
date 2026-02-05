package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// UsageEntry represents a single snapshot of NFS usage
type UsageEntry struct {
	Timestamp int64            `json:"timestamp"`
	Mounts    map[string]int64 `json:"mounts"`
	Total     int64            `json:"total"`
}

// isSnapshotMount returns true if the mount path contains ".snapshot"
func isSnapshotMount(mountPoint string) bool {
	return strings.Contains(mountPoint, ".snapshot")
}

// filterEntry returns a copy of the entry with .snapshot mounts removed and total recalculated
func filterEntry(entry UsageEntry) UsageEntry {
	filtered := UsageEntry{
		Timestamp: entry.Timestamp,
		Mounts:    make(map[string]int64),
		Total:     0,
	}
	for mount, bytes := range entry.Mounts {
		if !isSnapshotMount(mount) {
			filtered.Mounts[mount] = bytes
			filtered.Total += bytes
		}
	}
	return filtered
}

func main() {
	var filePath string
	var compare bool

	flag.StringVar(&filePath, "file", "", "Path to JSON file for storing usage data (default: CWD/nfsusage.json)")
	flag.StringVar(&filePath, "f", "", "Path to JSON file for storing usage data (shorthand)")
	flag.BoolVar(&compare, "compare", false, "Compare current usage with oldest entry")
	flag.BoolVar(&compare, "c", false, "Compare current usage with oldest entry (shorthand)")
	flag.Parse()

	// Set default file path
	if filePath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting current directory: %v\n", err)
			os.Exit(1)
		}
		filePath = filepath.Join(cwd, "nfsusage.json")
	}

	// Get NFS mounts
	nfsMounts, err := getNFSMounts()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting NFS mounts: %v\n", err)
		os.Exit(1)
	}

	if len(nfsMounts) == 0 {
		fmt.Fprintln(os.Stderr, "No NFS mounts found")
		os.Exit(0)
	}

	// Get usage for each mount
	currentEntry := UsageEntry{
		Timestamp: time.Now().Unix(),
		Mounts:    make(map[string]int64),
		Total:     0,
	}

	for _, mount := range nfsMounts {
		bytes, err := getDFBytes(mount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error getting df for %s: %v\n", mount, err)
			continue
		}
		currentEntry.Mounts[mount] = bytes
		currentEntry.Total += bytes
	}

	// Load existing entries
	entries, err := loadEntries(filePath)
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error loading existing data: %v\n", err)
		os.Exit(1)
	}

	// Append current entry
	entries = append(entries, currentEntry)

	// Save entries
	if err := saveEntries(filePath, entries); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving data: %v\n", err)
		os.Exit(1)
	}

	// Output to stdout
	if compare && len(entries) > 1 {
		// Filter oldest entry to exclude any .snapshot mounts that may exist in the JSON
		printComparison(filterEntry(entries[0]), currentEntry)
	} else {
		printCurrent(currentEntry)
	}
}

// getNFSMounts parses /proc/mounts to find NFS mount points (excludes .snapshot mounts)
func getNFSMounts() ([]string, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var mounts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 3 {
			fsType := fields[2]
			mountPoint := fields[1]
			if (fsType == "nfs" || fsType == "nfs4") && !isSnapshotMount(mountPoint) {
				mounts = append(mounts, mountPoint)
			}
		}
	}

	return mounts, scanner.Err()
}

// getDFBytes runs df on a mount point and returns the used bytes
func getDFBytes(mountPoint string) (int64, error) {
	cmd := exec.Command("df", "-B1", mountPoint)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("unexpected df output")
	}

	// df output may wrap to multiple lines if device name is long
	// Combine all non-header lines and parse
	dataLine := strings.Join(lines[1:], " ")
	fields := strings.Fields(dataLine)
	if len(fields) < 3 {
		return 0, fmt.Errorf("unexpected df output format")
	}

	// Field index 2 is "Used" when using -B1
	usedBytes, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing used bytes: %v", err)
	}

	return usedBytes, nil
}

// loadEntries loads existing entries from the JSON file
func loadEntries(filePath string) ([]UsageEntry, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var entries []UsageEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

// saveEntries saves entries to the JSON file
func saveEntries(filePath string, entries []UsageEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// formatBytes converts bytes to human readable format (GiB/TiB)
func formatBytes(bytes int64) string {
	const (
		GiB = 1024 * 1024 * 1024
		TiB = 1024 * GiB
	)

	if bytes >= TiB {
		return fmt.Sprintf("%.2f TiB", float64(bytes)/float64(TiB))
	}
	return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(GiB))
}

// formatDiff formats a byte difference with +/- prefix
func formatDiff(diff int64) string {
	if diff >= 0 {
		return "+" + formatBytes(diff)
	}
	return "-" + formatBytes(-diff)
}

// printCurrent prints the current usage with aligned columns
func printCurrent(entry UsageEntry) {
	// Calculate max mount point width
	maxMountWidth := len("total")
	for mount := range entry.Mounts {
		if len(mount) > maxMountWidth {
			maxMountWidth = len(mount)
		}
	}

	// Print mounts
	for mount, bytes := range entry.Mounts {
		fmt.Printf("%-*s  %s\n", maxMountWidth, mount, formatBytes(bytes))
	}
	fmt.Printf("%-*s  %s\n", maxMountWidth, "total", formatBytes(entry.Total))
}

// printComparison prints comparison between oldest and current entries with aligned columns
func printComparison(oldest, current UsageEntry) {
	// Build rows first to calculate column widths
	type row struct {
		mount, oldest, current, diff string
	}
	var rows []row

	// Collect all mounts from current entry
	for mount, currBytes := range current.Mounts {
		oldBytes := oldest.Mounts[mount]
		diff := currBytes - oldBytes
		rows = append(rows, row{mount, formatBytes(oldBytes), formatBytes(currBytes), formatDiff(diff)})
	}

	// Collect mounts that existed in oldest but not in current
	for mount, oldBytes := range oldest.Mounts {
		if _, exists := current.Mounts[mount]; !exists {
			rows = append(rows, row{mount, formatBytes(oldBytes), "(removed)", formatDiff(-oldBytes)})
		}
	}

	// Add total row
	diff := current.Total - oldest.Total
	rows = append(rows, row{"total", formatBytes(oldest.Total), formatBytes(current.Total), formatDiff(diff)})

	// Calculate column widths
	mountWidth := len("Mountpoint")
	oldestWidth := len("Oldest")
	currentWidth := len("Current")
	diffWidth := len("Difference")

	for _, r := range rows {
		if len(r.mount) > mountWidth {
			mountWidth = len(r.mount)
		}
		if len(r.oldest) > oldestWidth {
			oldestWidth = len(r.oldest)
		}
		if len(r.current) > currentWidth {
			currentWidth = len(r.current)
		}
		if len(r.diff) > diffWidth {
			diffWidth = len(r.diff)
		}
	}

	// Print header
	fmt.Printf("%-*s  %*s  %*s  %*s\n", mountWidth, "Mountpoint", oldestWidth, "Oldest", currentWidth, "Current", diffWidth, "Difference")
	fmt.Printf("%-*s  %*s  %*s  %*s\n", mountWidth, strings.Repeat("-", mountWidth), oldestWidth, strings.Repeat("-", oldestWidth), currentWidth, strings.Repeat("-", currentWidth), diffWidth, strings.Repeat("-", diffWidth))

	// Print rows
	for _, r := range rows {
		fmt.Printf("%-*s  %*s  %*s  %*s\n", mountWidth, r.mount, oldestWidth, r.oldest, currentWidth, r.current, diffWidth, r.diff)
	}
}
