#!/usr/bin/env python
#
# image-usb-stick
#  
# Author:
#   Aaron Bockover <abockover@novell.com>
# 
# Copyright 2010 Novell, Inc.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

import os
import sys
import re
from time import time
from optparse import OptionParser

# ------------------------------------------------------------------------
# Utilities for the Disk classes
# ------------------------------------------------------------------------

def sh (cmd):
	for line in os.popen (cmd).readlines ():
		yield line.rstrip ('\r\n')

def make_id (id):
	return re.sub ('[_]{2,}', '_',
		re.sub ('^[0-9]', '_',
			re.sub ('[^A-Za-z0-9]', '_', id))) \
			.strip () \
			.strip ('_') \
			.lower ()

# ------------------------------------------------------------------------
# Base Disk classes
# ------------------------------------------------------------------------

class Disk:
	def __init__ (self):
		self.device_node = None
		self.mount_point = None
		self.is_mounted = False
		self.name = 'Unknown'
		self.size = 0
		self.children = []

	def unmount (self):
		return False
	
	def eject (self):
		return False

	def is_valid (self):
		return not self.device_node == None and self.size > 0

	def get_mounted_devices (self):
		if self.is_mounted:
			yield self
		for child in self.children:
			if child.is_mounted:
				yield child
	
	def __str__ (self):
		return '%s (%s) - %0.02g GB' % (self.name,
			self.device_node, self.size / 1000000000.0)

class DiskManager:
	@staticmethod
	def get_manager ():
		import types
		for name, type in globals ().iteritems ():
			if isinstance (type, types.ClassType) and \
				issubclass (type, DiskManager) and \
				not type == DiskManager:
				try:
					return type ()
				except:
					pass

	def get_devices (self):
		return

# ------------------------------------------------------------------------
# OS X Disk implementation
# ------------------------------------------------------------------------

class OsxDisk (Disk):
	def __init__ (self, node, parent = None):
		Disk.__init__ (self)

		self.attrs = {}
		self.parent = parent
		
		for parts in [line.split (':') for line in sh ('diskutil info %s' % node)]:
			if len (parts) == 2:
				val = parts[1].strip ()
				if len (val) > 0:
					self.attrs[make_id (parts[0])] = val
		
		self.device_node = self.attrs['device_node']
		self.is_mounted = 'mounted' in self.attrs and self.attrs['mounted'] == 'Yes'
		self.name = self.attrs['device_media_name']
		
		if 'volume_name' in self.attrs and not self.attrs['volume_name'] == self.name:
			self.name = '%s (%s)' % (self.name, self.attrs['volume_name'])
		
		if 'mount_point' in self.attrs:
			self.mount_point = self.attrs['mount_point']
		
		m = re.match ('.*\((\d+) Bytes\).*', self.attrs['total_size'])
		if m:
			self.size = float (m.group (1))

		if self.parent:
			return
		
		for parts in [line.split (':') for line in sh ('diskutil list %s' % node)]:
			try:
				id = int (parts[0].strip ())
				if id < 1:
					continue
			except:
				continue

			child = OsxDisk ('%ss%d' % (self.device_node, id), self)
			self.children.append (child)

	def is_valid (self):
		return \
			Disk.is_valid (self) and \
			self.attrs['device_identifier'] == self.attrs['part_of_whole'] and \
			self.attrs['protocol'] == 'USB' and \
			self.attrs['read_only_media'] == 'No' and \
			self.attrs['ejectable'] == 'Yes' and \
			self.attrs['whole'] == 'Yes' and \
			self.attrs['internal'] == 'No'

	def unmount (self):
		import subprocess
		proc = subprocess.Popen ('diskutil unmount %s' % self.device_node, shell = True)
		return os.waitpid (proc.pid, 0)[1] == 0	

class OsxDiskManager (DiskManager):
	def __init__ (self):
		if not os.path.isdir ('/Volumes') or \
			not os.path.isdir ('/System') or \
			not os.path.isfile ('/usr/sbin/diskutil'):
			raise Exception ()

	def get_devices (self):
		for node in sh ('diskutil list'):
			device = self.get_device (node)
			if device:
				yield device

	def get_device (self, node):
		if node.startswith ('/dev/'):
			try:
				disk = OsxDisk (node)
				if disk.is_valid ():
					return disk
			except:
				pass

# ------------------------------------------------------------------------
# udevadm Disk implementation
# ------------------------------------------------------------------------

class UdevDevice:
	def __init__ (self):
		self.path = None
		self.name = None
		self.symlinks = []
		self.properties = {}

class UdevDeviceManager:
	def __init__ (self):
		if not os.path.isdir ('/sys') or \
			not os.path.isfile ('/sbin/udevadm'):
			raise Exception ()

		self.devices = []
		device = None
		for line in sh ('/sbin/udevadm info --root --export-db'):
			try:
				key, value = line.split (':', 1)
			except:
				continue
			value = value.strip ()
			if key == 'P':
				if device:
					self.devices.append (device)
				device = UdevDevice ()
				device.path = value
			elif not device:
				continue
			elif key == 'N':
				device.name = value
			elif key == 'S':
				device.symlinks.append (value)
			elif key == 'E':
				key, value = value.split ('=', 1)
				device.properties[key.strip ()] = value.strip ()
		if device:
			self.devices.append (device)

	def query_by_properties (self, *matches):
		for dev in self.devices:
			match = True
			for match in matches:
				if isinstance (match, str):
					if match not in dev.properties:
						match = False
						break
				elif isinstance (match, (list, tuple)):
					k, v = match[:2]
					if k not in dev.properties or not dev.properties[k] == v:
						match = False
						break
			if match:
				yield dev

class UdevDisk (Disk):
	def __init__ (self, udev_device, udev_manager):
		Disk.__init__ (self)
		self.device_node = udev_device.properties['DEVNAME']
		if not self.device_node[0] == '/':
			self.device_node = '/dev/%s' % self.device_node

		self.name = udev_device.properties['ID_MODEL']

		try:
			with open ('/sys/class/block/%s/size' % \
				os.path.basename (self.device_node), 'r') as fp:
				self.size = int (fp.read ()) * 512
		except:
			pass

		try:
			self.mount_point = udev_manager.mounts[self.device_node]
			self.is_mounted = not self.mount_point == None
			print self.mount_point
		except:
			pass

		if not udev_device.properties['DEVTYPE'] == 'disk':
			return

		for child in udev_manager.query_by_properties (
			['ID_BUS', 'usb'],
			['ID_TYPE', 'disk'],
			['DEVTYPE', 'partition'],
			['ID_SERIAL', udev_device.properties['ID_SERIAL']]):
			try:
				child_disk = UdevDisk (child, udev_manager)
				if child_disk.is_valid ():
					self.children.append (child_disk)
			except:
				pass

	def is_valid (self):
		return Disk.is_valid (self) and os.path.exists (self.device_node)

	def unmount (self):
		import subprocess
		proc = subprocess.Popen ('/bin/umount %s' % self.device_node, shell = True)
		return os.waitpid (proc.pid, 0)[1] == 0

class UdevDiskMianager (DiskManager, UdevDeviceManager):
	def __init__ (self):
		UdevDeviceManager.__init__ (self)

		self.mounts = {}
		try:
			for line in sh ('/bin/mount'):
				parts = line.split ()
				if len (parts) >= 3 and parts[1] == 'on':
					self.mounts[parts[0]] = parts[2]
		except:
			pass

	def get_devices (self):
		for dev in self.query_by_properties (
			['ID_BUS', 'usb'],
			['ID_TYPE', 'disk'],
			['DEVTYPE', 'disk']):
			try:
				disk = UdevDisk (dev, self)
				if disk.is_valid ():
					yield disk
			except:
				pass

# ------------------------------------------------------------------------
# Tool utilities
# ------------------------------------------------------------------------

def abort ():
	print >> sys.stderr, '\nAborted'
	sys.exit (1)

def prompt (prompt, validate):
	while True:
		try:
			result = validate (raw_input (prompt))
			if result:
				return result
		except (KeyboardInterrupt, EOFError):
			abort ()
		except:
			pass

def image (in_path, out_path, progress_cb = None):
	file_size = os.stat (in_path).st_size
	buf_size = 4096
	try:
		buf_size = os.stat (out_path).st_blksize
	except:
		pass

	bytes_read = 0
	progress = 0
	last_raise_time = 0
	start_time = time ()

	with open (in_path, 'rb') as in_fp:
		with open (out_path, 'wb') as out_fp:
			while True:
				buf = bytearray (buf_size)
				r = in_fp.readinto (buf) 
				if r < buf_size:
					buf = buf[:r]
				out_fp.write (buf)

				bytes_read += r
				progress = int ((bytes_read / float (file_size)) * 100)

				current_time = time ()
				if progress_cb and (r < buf_size or \
					last_raise_time == 0 or current_time - last_raise_time > 1):
					last_raise_time = current_time
					progress_cb (progress, start_time, bytes_read, file_size)
				
				if r < buf_size:
					break

			out_fp.flush ()

def calc_eta (bytes_read, bytes_total, elapsed):
	if bytes_read < 1:
		return 0	
	return long (((bytes_total - bytes_read) * elapsed) / bytes_read)

def calc_bar (progress, length):
	fill = int ((progress / 100.0) * length)
	empty = length - fill
	return '=' * fill + ' ' * empty

def progress (progress, start_time, bytes_read, total_bytes):
	elapsed = time () - start_time
	eta = calc_eta (bytes_read, total_bytes, elapsed)
	bar = calc_bar (progress, 48)
	sys.stdout.write ('\r%3d%%  %ld:%02ld:%02ld  [%s]  ETA %ld:%02ld:%02ld ' % \
		(progress,
			elapsed / 3600, (elapsed / 60) % 60, elapsed % 60,
			bar,
			eta / 3600, (eta / 60) % 60, eta % 60)) 
	sys.stdout.flush ()

# ------------------------------------------------------------------------
# Main program
# ------------------------------------------------------------------------

if __name__ == '__main__':
	parser = OptionParser (
		usage = 'Usage: %prog [options] IMAGE_FILE'
	)
	parser.add_option ('-d', '--device',
		action = 'store',
		dest = 'device',
		help = 'Manually selected device node. This device node must be a valid root level USB storage device node even if manually selected. Omitting this option will present a menu of valid nodes.'
	)
	parser.add_option ('-f', '--force',
		action = 'store_true',
		dest = 'force',
		help = 'Force the writing of the image to device. This option will not prompt for confirmation before writing to the device, and implies the -u|--unmount option!'
	)
	parser.add_option ('-u', '--unmount',
		action = 'store_true',
		dest = 'unmount',
		help = 'Unmount any mounted partitions on the device. This option will not prompt for unmounting any mounted partitions.')
	parser.add_option ('-s', '--checksum',
		action = 'store',
		dest = 'checksum',
		help = 'Checksum of IMAGE_FILE. This checksum may be prefixed with a hash type. For instance, \'md5:abc...\', \'sha1:abc...\', \'sha512:abc...\'; if no prefix is specified, md5 is assumed for the hash type.'
	)

	options, args = parser.parse_args ()

	if len (args) < 1:
		parser.print_help ()
		sys.exit (1)

	if not os.path.isfile (args[0]):
		sys.exit ('File not found: %s' % args[0])

	print 'Loading disks...'

	manager = DiskManager.get_manager ()
	devices = [d for d in manager.get_devices ()]
	target_device = None

	if len (devices) == 0:
		sys.exit ('No devices found.')

	print

	if options.device:
		for device in devices:
			if device.device_node == options.device:
				target_device = device
				break

		if not target_device:
			sys.exit ('Invalid USB device node: %s' % options.device)
	else:
		print 'Select a device to image:\n'
		for i in range (0, len (devices)):
			print '  %d) %s' % (i + 1, devices[i])
			for child in devices[i].children:
				print '     - %s' % child
			print
		def select (i):
			if i >= 1 and i <= len (devices):
				return devices[i - 1]
		target_device = prompt ('Choice: ', lambda i: select (int (i)))
	
	print '\nSelected: %s\n' % target_device

	def select_yes_no (i):
		i = i.lower ()
		if i in ('y', 'yes'):
			return 'y'
		elif i in ('n', 'no'):
			return 'n'

	mounts = [m for m in target_device.get_mounted_devices ()]
	if len (mounts) > 0:
		print 'Device has one or more mounted partitions:\n'
		for m in mounts:
			print '  %s @ %s' % (m.device_node, m.mount_point)
		print
		if options.unmount or options.force or \
			prompt ('Unmount all partitions? [Y/N]: ', select_yes_no) == 'y':
			for m in mounts:
				if not m.unmount ():
					sys.exit ('Failed to unmount device: %s at %s' % \
						(m.device_node, m.mount_point))

	if options.force or prompt ('WARNING: imaging %s may result in data loss! Continue? [Y/N]: ' \
		% target_device.device_node, select_yes_no) == 'y':
		image (args[0], target_device.device_node, progress)
		print
		print 'Done.'
	else:
		abort ()
