package multipath

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wlynxg/anet"
)

// Addr interface for compatibility
type Addr interface {
	Network() string // name of the network (for example, "tcp", "udp")
	String() string  // string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
}

// anet-compatible flag constants (matching net.Flags values)
const (
	anetFlagUp           = 1 << iota // interface is up
	anetFlagBroadcast                // interface supports broadcast access capability
	anetFlagLoopback                 // interface is a loopback interface
	anetFlagPointToPoint             // interface belongs to a point-to-point link
	anetFlagMulticast                // interface supports multicast access capability
)

// Conversion function to convert net.Addr to our Addr interface
func convertAnetAddrs(addrs []net.Addr) []Addr {
	result := make([]Addr, len(addrs))
	for i, addr := range addrs {
		result[i] = addr
	}
	return result
}

// Helper functions to avoid net package usage
func (im *InterfaceMonitor) extractIPFromAddr(addrStr string) string {
	// Handle CIDR notation (e.g., "192.168.1.1/24")
	if idx := strings.Index(addrStr, "/"); idx != -1 {
		return addrStr[:idx]
	}
	return addrStr
}

func (im *InterfaceMonitor) isLoopbackIP(ipStr string) bool {
	return strings.HasPrefix(ipStr, "127.") || ipStr == "::1"
}

func (im *InterfaceMonitor) isLinkLocalIP(ipStr string) bool {
	// IPv4 link-local: 169.254.0.0/16
	if strings.HasPrefix(ipStr, "169.254.") {
		return true
	}
	// IPv6 link-local: fe80::/10
	if strings.HasPrefix(ipStr, "fe80:") {
		return true
	}
	return false
}

func (im *InterfaceMonitor) isMulticastIP(ipStr string) bool {
	// IPv4 multicast: 224.0.0.0/4
	if strings.HasPrefix(ipStr, "224.") || strings.HasPrefix(ipStr, "225.") ||
		strings.HasPrefix(ipStr, "226.") || strings.HasPrefix(ipStr, "227.") ||
		strings.HasPrefix(ipStr, "228.") || strings.HasPrefix(ipStr, "229.") ||
		strings.HasPrefix(ipStr, "230.") || strings.HasPrefix(ipStr, "231.") ||
		strings.HasPrefix(ipStr, "232.") || strings.HasPrefix(ipStr, "233.") ||
		strings.HasPrefix(ipStr, "234.") || strings.HasPrefix(ipStr, "235.") ||
		strings.HasPrefix(ipStr, "236.") || strings.HasPrefix(ipStr, "237.") ||
		strings.HasPrefix(ipStr, "238.") || strings.HasPrefix(ipStr, "239.") {
		return true
	}
	// IPv6 multicast: ff00::/8
	if strings.HasPrefix(ipStr, "ff") {
		return true
	}
	return false
}

func (im *InterfaceMonitor) isValidIP(ipStr string) bool {
	// Use net.ParseIP for validation since we're allowed to use net for parsing
	ip := net.ParseIP(ipStr)
	return ip != nil
}

// InterfaceMonitor monitors network interfaces for changes and manages dynamic subflows
type InterfaceMonitor struct {
	mpConn          *mpConn
	monitorInterval time.Duration
	lastInterfaces  map[string]*InterfaceInfo
	muInterfaces    sync.RWMutex
	stopChan        chan struct{}
	enabled         bool
}

// InterfaceInfo represents information about a network interface
type InterfaceInfo struct {
	Name       string
	Index      int
	Addresses  []string
	IsUp       bool
	IsLoopback bool
	LastSeen   time.Time
}

// NewInterfaceMonitor creates a new interface monitor
func NewInterfaceMonitor(mpConn *mpConn) *InterfaceMonitor {
	return &InterfaceMonitor{
		mpConn:          mpConn,
		monitorInterval: 7 * time.Second, // 5-10 seconds as requested
		lastInterfaces:  make(map[string]*InterfaceInfo),
		stopChan:        make(chan struct{}),
		enabled:         true,
	}
}

// Start begins monitoring network interfaces
func (im *InterfaceMonitor) Start() {
	if !im.enabled {
		return
	}

	log.Debugf("Starting network interface monitoring (interval: %v)", im.monitorInterval)

	// Initial scan
	im.scanInterfaces()

	// Start periodic monitoring
	ticker := time.NewTicker(im.monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			im.scanInterfaces()
		case <-im.stopChan:
			log.Debugf("Stopping network interface monitoring")
			return
		}
	}
}

// Stop stops the interface monitoring
func (im *InterfaceMonitor) Stop() {
	close(im.stopChan)
}

// scanInterfaces scans all network interfaces and detects changes
func (im *InterfaceMonitor) scanInterfaces() {
	// Get current interfaces using anet
	anetInterfaces, err := anet.Interfaces()
	if err != nil {
		log.Errorf("Failed to get network interfaces: %v", err)
		return
	}

	currentInterfaces := make(map[string]*InterfaceInfo)

	// Process each interface
	for _, iface := range anetInterfaces {
		info := im.processInterface(iface)
		if info != nil {
			currentInterfaces[info.Name] = info
		}
	}

	// Detect changes
	im.detectChanges(currentInterfaces)

	// Update last known interfaces
	im.muInterfaces.Lock()
	im.lastInterfaces = currentInterfaces
	im.muInterfaces.Unlock()
}

// processInterface processes a single network interface
func (im *InterfaceMonitor) processInterface(iface net.Interface) *InterfaceInfo {
	// Skip loopback interfaces
	if iface.Flags&net.FlagLoopback != 0 {
		return nil
	}

	// Skip interfaces that are down
	if iface.Flags&net.FlagUp == 0 {
		return nil
	}

	// Get addresses for this interface using anet
	addresses, err := anet.InterfaceAddrsByInterface(&iface)
	if err != nil {
		log.Debugf("Failed to get addresses for interface %s: %v", iface.Name, err)
		return nil
	}

	// Convert addresses to our local type
	localAddresses := convertAnetAddrs(addresses)

	// Filter usable addresses
	usableAddresses := im.filterUsableAddresses(localAddresses)
	if len(usableAddresses) == 0 {
		return nil
	}

	return &InterfaceInfo{
		Name:       iface.Name,
		Index:      iface.Index,
		Addresses:  usableAddresses,
		IsUp:       iface.Flags&net.FlagUp != 0,
		IsLoopback: iface.Flags&net.FlagLoopback != 0,
		LastSeen:   time.Now(),
	}
}

// filterUsableAddresses filters out loopback and invalid addresses
func (im *InterfaceMonitor) filterUsableAddresses(addresses []Addr) []string {
	var usable []string

	for _, addr := range addresses {
		// Parse the address using net.ParseCIDR
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			continue
		}

		// Skip loopback addresses
		if ip.IsLoopback() {
			continue
		}

		// Skip link-local addresses
		if ip.IsLinkLocalUnicast() {
			continue
		}

		// Skip multicast addresses
		if ip.IsMulticast() {
			continue
		}

		// Only include IPv4 and IPv6 addresses
		if ip.To4() != nil || ip.To16() != nil {
			usable = append(usable, ip.String())
		}
	}

	return usable
}

// detectChanges detects interface changes and triggers appropriate actions
func (im *InterfaceMonitor) detectChanges(currentInterfaces map[string]*InterfaceInfo) {
	im.muInterfaces.RLock()
	lastInterfaces := im.lastInterfaces
	im.muInterfaces.RUnlock()

	// Detect new interfaces
	for name, currentInfo := range currentInterfaces {
		if _, exists := lastInterfaces[name]; !exists {
			log.Debugf("New network interface detected: %s with addresses %v", name, currentInfo.Addresses)
			im.handleNewInterface(currentInfo)
		} else {
			// Check for address changes
			lastInfo := lastInterfaces[name]
			if im.addressesChanged(lastInfo.Addresses, currentInfo.Addresses) {
				log.Debugf("Interface %s addresses changed: %v -> %v", name, lastInfo.Addresses, currentInfo.Addresses)
				im.handleInterfaceChange(lastInfo, currentInfo)
			}
		}
	}

	// Detect removed interfaces
	for name, lastInfo := range lastInterfaces {
		if _, exists := currentInterfaces[name]; !exists {
			log.Debugf("Network interface removed: %s", name)
			im.handleRemovedInterface(lastInfo)
		}
	}
}

// addressesChanged checks if the address list has changed
func (im *InterfaceMonitor) addressesChanged(old, new []string) bool {
	if len(old) != len(new) {
		return true
	}

	oldMap := make(map[string]bool)
	for _, addr := range old {
		oldMap[addr] = true
	}

	for _, addr := range new {
		if !oldMap[addr] {
			return true
		}
	}

	return false
}

// handleNewInterface handles the detection of a new network interface
func (im *InterfaceMonitor) handleNewInterface(info *InterfaceInfo) {
	// Add new subflows for each address on the new interface
	for _, address := range info.Addresses {
		im.addSubflowForAddress(address, info.Name)
	}
}

// handleInterfaceChange handles changes to an existing interface
func (im *InterfaceMonitor) handleInterfaceChange(oldInfo, newInfo *InterfaceInfo) {
	// Find addresses that were removed
	oldMap := make(map[string]bool)
	for _, addr := range oldInfo.Addresses {
		oldMap[addr] = true
	}

	// Remove subflows for addresses that no longer exist
	for _, addr := range oldInfo.Addresses {
		if !oldMap[addr] {
			im.removeSubflowForAddress(addr)
		}
	}

	// Add subflows for new addresses
	for _, addr := range newInfo.Addresses {
		if !oldMap[addr] {
			im.addSubflowForAddress(addr, newInfo.Name)
		}
	}
}

// handleRemovedInterface handles the removal of a network interface
func (im *InterfaceMonitor) handleRemovedInterface(info *InterfaceInfo) {
	// Remove all subflows for addresses on the removed interface
	for _, address := range info.Addresses {
		im.removeSubflowForAddress(address)
	}
}

// addSubflowForAddress adds a new subflow for the given address
func (im *InterfaceMonitor) addSubflowForAddress(address, interfaceName string) {
	// First validate that the address is actually usable for outgoing connections
	if !im.isAddressUsableForOutgoing(address) {
		log.Debugf("Address %s is not usable for outgoing connections, skipping", address)
		return
	}

	// Get the original dialers to find server addresses
	im.mpConn.muFailedSubflows.RLock()
	originalDialers := im.mpConn.originalDialers
	im.mpConn.muFailedSubflows.RUnlock()

	if len(originalDialers) == 0 {
		log.Debugf("No original dialers available for dynamic subflow creation")
		return
	}

	// Try to connect to each server address using the new local interface
	for _, dialer := range originalDialers {
		// Extract server address from dialer label
		serverAddr := im.extractServerAddress(dialer.Label())
		if serverAddr == "" {
			continue
		}

		// Create a new dialer that binds to the specific local address
		localDialer := &boundDialer{
			localAddr:  address,
			serverAddr: serverAddr,
		}

		// Attempt to connect
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := localDialer.DialContext(ctx)
		cancel()

		if err != nil {
			log.Debugf("Failed to connect from %s to %s: %v", address, serverAddr, err)
			continue
		}

		// Add the new dynamic subflow
		probeStart := time.Now()
		tracker := &NullTracker{}
		subflowName := fmt.Sprintf("dynamic-%s-%s", address, serverAddr)
		im.mpConn.addDynamicSubflow(subflowName, conn, true, probeStart, tracker, address, interfaceName)

		log.Debugf("Added new subflow for local address %s to server %s on interface %s", address, serverAddr, interfaceName)
	}
}

// extractServerAddress extracts the server address from a dialer label
func (im *InterfaceMonitor) extractServerAddress(label string) string {
	// Try multiple common dialer label formats
	patterns := []string{
		"TCP dialer to ", // "TCP dialer to localhost:8080"
		"dialer to ",     // "dialer to localhost:8080"
		"tcp dialer to ", // "tcp dialer to localhost:8080"
		"bound-dialer-",  // "bound-dialer-192.168.1.1->localhost:8080"
		"dynamic-tcp-",   // "dynamic-tcp-localhost:8080"
	}

	for _, pattern := range patterns {
		if len(label) > len(pattern) && label[:len(pattern)] == pattern {
			address := label[len(pattern):]

			// For bound-dialer format, extract the server part after "->"
			if pattern == "bound-dialer-" {
				if idx := strings.Index(address, "->"); idx != -1 {
					address = address[idx+2:]
				}
			}

			// Validate the extracted address
			if im.isValidAddress(address) {
				return address
			}
		}
	}

	// If no pattern matches, try to extract any address-like string
	return im.extractAddressFromString(label)
}

// isValidAddress validates if the string looks like a valid network address
func (im *InterfaceMonitor) isValidAddress(addr string) bool {
	// Check if it contains a port (has colon and looks like host:port)
	if strings.Contains(addr, ":") {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			// Check if the port part is numeric
			if _, err := strconv.Atoi(parts[1]); err == nil {
				return true
			}
		}
	}
	return false
}

// extractAddressFromString tries to extract an address from any string
func (im *InterfaceMonitor) extractAddressFromString(s string) string {
	// Look for patterns like "host:port" in the string
	re := regexp.MustCompile(`([a-zA-Z0-9.-]+):(\d+)`)
	matches := re.FindStringSubmatch(s)
	if len(matches) >= 3 {
		return matches[0] // Return the full match (host:port)
	}
	return ""
}

// isAddressUsableForOutgoing checks if an address can be used for outgoing connections
func (im *InterfaceMonitor) isAddressUsableForOutgoing(address string) bool {
	// Parse the address to check if it's valid
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}

	// Skip loopback addresses for outgoing connections (these are for local communication only)
	if ip.IsLoopback() {
		return false
	}

	// Skip link-local addresses (these are only for same-link communication)
	if ip.IsLinkLocalUnicast() {
		return false
	}

	// Skip multicast addresses (these are for group communication, not point-to-point)
	if ip.IsMulticast() {
		return false
	}

	// For IPv6, skip link-local addresses (fe80::/10) - these are only for same-link communication
	if ip.To4() == nil { // IPv6
		if len(ip) == 16 && ip[0] == 0xfe && (ip[1]&0xc0) == 0x80 {
			return false
		}
		// Note: We now allow unique local addresses (fc00::/7) as they can be used behind NAT
	}

	// For IPv4, we now allow all private ranges as they can be used behind NAT:
	// - 10.0.0.0/8 (Class A private)
	// - 172.16.0.0/12 (Class B private)
	// - 192.168.0.0/16 (Class C private)
	// These are all valid for outgoing connections when behind NAT

	// For now, assume all non-loopback, non-link-local, non-multicast addresses are usable
	// The runtime test was too aggressive and rejected valid addresses behind NAT
	return true
}

// testAddressUsability performs a quick test to see if an address can be used for outgoing connections
func (im *InterfaceMonitor) testAddressUsability(address string) bool {
	// Parse the address to determine the appropriate test destination
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}

	var testDest string
	if ip.To4() != nil {
		// For IPv4, try to connect to a public DNS server (8.8.8.8:53)
		// This tests if the address can reach external networks
		testDest = "8.8.8.8:53"
	} else {
		// For IPv6, try to connect to a public IPv6 DNS server (2001:4860:4860::8888:53)
		testDest = "[2001:4860:4860::8888]:53"
	}

	// Try to create a test dialer with this local address
	testDialer := &boundDialer{
		localAddr:  address,
		serverAddr: testDest,
	}

	// Try to create the dialer (this will fail at connection time, but we can check if the local address is valid)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := testDialer.DialContext(ctx)

	// We expect the connection to fail, but we want to check if the error is about the local address
	// If the error is about the local address being invalid, then this address is not usable
	if err != nil {
		errStr := err.Error()
		// Check for common "address not valid" errors
		if strings.Contains(errStr, "not valid") ||
			strings.Contains(errStr, "invalid") ||
			strings.Contains(errStr, "cannot assign") ||
			strings.Contains(errStr, "no such device") ||
			strings.Contains(errStr, "no route to host") {
			return false
		}
		// Other errors (like connection refused, timeout, etc.) are expected and mean the address is valid
		return true
	}

	return true
}

// removeSubflowForAddress removes a subflow for the given address
func (im *InterfaceMonitor) removeSubflowForAddress(address string) {
	// Find and close all subflows with this local address
	im.mpConn.muSubflows.RLock()
	var toRemove []*subflow
	for _, sf := range im.mpConn.subflows {
		// Check if this is a dynamic subflow using the specified local address
		if sf.isDynamicSubflow() && sf.getLocalAddress() == address {
			toRemove = append(toRemove, sf)
		}
	}
	im.mpConn.muSubflows.RUnlock()

	// Close the subflows
	for _, sf := range toRemove {
		log.Debugf("Removing dynamic subflow %s for local address %s on interface %s", sf.to, address, sf.getInterfaceName())
		go sf.close()
	}
}

// boundDialer is a dialer that binds to a specific local address
type boundDialer struct {
	localAddr  string
	serverAddr string
}

func (d *boundDialer) DialContext(ctx context.Context) (net.Conn, error) {
	// Parse local address - handle IPv6 properly
	var localAddr *net.TCPAddr
	var err error

	// Check if it's an IPv6 address (contains colons)
	if strings.Contains(d.localAddr, ":") {
		// For IPv6, we need to wrap in brackets and add port
		localAddr, err = net.ResolveTCPAddr("tcp", "["+d.localAddr+"]:0")
	} else {
		// For IPv4, just append port
		localAddr, err = net.ResolveTCPAddr("tcp", d.localAddr+":0")
	}

	if err != nil {
		return nil, err
	}

	// Create dialer with local address binding
	dialer := &net.Dialer{
		LocalAddr: localAddr,
		Timeout:   2 * time.Second,
	}

	return dialer.DialContext(ctx, "tcp", d.serverAddr)
}

func (d *boundDialer) Label() string {
	return fmt.Sprintf("bound-dialer-%s->%s", d.localAddr, d.serverAddr)
}
