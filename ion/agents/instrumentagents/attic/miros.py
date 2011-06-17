import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# $Id$

# ==================================================================
"""

Miros - A Python module that implements a Hierarchical State Machine (HSM)
class (i.e. one that implements behavioral inheritance). 

It is based on the excellent work of Miro Samek (hence the module name
"miros"). This implementation closely follows an older C/C++
implementation published in the 8/00 edition of "Embedded Systems"
magazine by Miro Samek and Paul Montgomery under the title "State
Oriented Programming". The article and code can be found here:
http://www.embedded.com/2000/0008.

A wealth of more current information can be found at Miro's well kept
site: http://www.state-machine.com/.
 
As far as I know this is the first implementation of Samek's work in
Python. It was tested with Python 2.5

It is licensed under the same terms as Python itself.

-----------------------------------------------------------
Change history.
Date        Comments
-----------------------------------------------------------

2/15/09    Tom Schmit, Began porting from Lua version.
2/22/09    TS, test_non_interactive() and test_interactive() run as expected.

"""
# ===================================================================

# =========================================================
	
# ===============================================
#

class Hsm():

	# revision = "$Revision$"[11:].strip(" $")
	revision = "1164"

	# ====================================
	# Instantiates a new HSM.

	def __init__(self):
		self.hsm = {}
		# import pprint
		# self.pp = pprint.PrettyPrinter(indent = 4)

	# ====================================
	# Adds a new state to the HSM. Specifies name, handler function, parent
	# state, and initializes an empty cache for toLca. These are added to Hsm
	# as a sub-table that is indexed by a reference to the associated handler
	# function. Note that toLca is computed when state transitions. It is the
	# number of levels between the source state and the least/lowest common
	# ancestor is shares with the target state.

	def addState(self, sName, fHandler, fSuper):
		self.hsm[fHandler] = dict( 
			name = sName, 
			handler = fHandler, 
			super = fSuper,
			toLcaCache = {}
			# toLca = 0
		)
		# print("addState:", sType, self.hsm[fHandler], fHandler, fSuper)

	# ====================================
	# Displays parent-child relationship of states.

	def dump(self):
		# self.pp.pprint(self.hsm)
		print
		for state in self.hsm:
			print "State: " + self.hsm[state]['name'] + "\t", self.hsm[state]

	# 
	# ====================================
	# Starts HSM. Enters and starts top state.

	def onStart(self, top):
		# expandable table containing event parameters sent to HSM
		self.tEvt = {'sType': "entry", 'nFoo': 0}
		
		self.rCurr = self.hsm[top]
		# self.pp.pprint(self.rCurr)
		self.rNext = 0
		# handler for top sets initial state
		self.rCurr['handler'](self)
		# get path from initial state to top/root state
		while True:
			self.tEvt['sType'] = "init"
			self.rCurr['handler'](self)
			if self.rNext == 0:
				break
			entryPath = []
			s = self.rNext
			while s != self.rCurr:
			# while s != self.rCurr and s['super'] != None:
				# trace path to target
				entryPath.append(s['handler'])
				s = self.hsm[s['super']]
				# s = self.hsm.get(s['super'], None)
			# follow path in reverse calling each handler
			self.tEvt['sType'] = "entry"
			entryPath.reverse()
			for h in entryPath:
				# retrace entry from source
				# self.pp.pprint(h)
				h(self)
			self.rCurr = self.rNext
			self.rNext = 0

	# 
	# ====================================
	# Dispatches events.

	def onEvent(self, sType):
		self.tEvt['sType'] = sType
		s = self.rCurr
		while True:
			# level of outermost event handler
			self.rSource = s
			self.tEvt['sType'] = s['handler'](self)
			# processed?
			if self.tEvt['sType'] == 0:
				# state transition taken?
				# print self.rNext
				if self.rNext != 0:
					entryPath = []
					s = self.rNext
					while s != self.rCurr:
						# while s != self.rCurr and s['super'] != None:
						# trace path to target
						entryPath.append(s['handler'])
						s = self.hsm[s['super']]
						# s = self.hsm.get(s['super'], None)
					# follow path in reverse from LCA calling each handler
					self.tEvt['sType'] = "entry"
					entryPath.reverse()
					for h in entryPath:
						# retrace entry from source
						h(self)
					self.rCurr = self.rNext
					self.rNext = 0
					while True:
						self.tEvt['sType'] = "init"
						self.rCurr['handler'](self)
						if self.rNext == 0:
							break
						entryPath = []
						s = self.rNext
						while s != self.rCurr:
						# while s != self.rCurr and s['super'] != None:
							# record path to target
							entryPath.append(s['handler'])
							s = self.hsm[s['super']]
							# s = self.hsm.get(s['super'], None)
						# follow path in reverse calling each handler
						self.tEvt['sType'] = "entry"
						entryPath.reverse()
						for h in entryPath:
							# retrace entry
							h(self)
						self.rCurr = self.rNext
						self.rNext = 0
				# event processed
				break
			s = self.hsm[s['super']]
			# s = self.hsm.get(s['super'], None)
		return 0

	# 
	# ====================================
	# Exits current states and all super states up to LCA.

	def exit(self, toLca):
		s = self.rCurr
		self.tEvt['sType'] = "exit"
		while s != self.rSource:
			# while s != self.rSource and s['super'] != None:
			s['handler'](self)
			s = self.hsm[s['super']]
			# s = self.hsm.get(s['super'], None)
		while toLca != 0:
			toLca = toLca - 1
			s['handler'](self)
			s = self.hsm[s['super']]
			# s = self.hsm.get(s['super'], None)
		self.rCurr = s

	# ====================================
	# Finds number of levels to LCA (least common ancestor). 

	def toLCA(self, Target):
		toLca = 0
		if self.rSource == Target:
			return 1
		s = self.rSource
		while s != None:
			t = Target
			while t != None:
				if s == t:
					return toLca
				t = self.hsm.get(t['super'], None)
			toLca = toLca + 1
			s = self.hsm.get(s['super'], None)
		return 0

	# ====================================
	# Transitions to new state.

	# ==========================
	# Cached version.

	def stateTran(self, rTarget):
		# self.pp.pprint(self.rCurr)
		# print "\nCurrent state: ", self.rCurr; print "Source state: ", self.rSource
		if self.rCurr['toLcaCache'].get(self.tEvt['sType'], None) == None:
			self.rCurr['toLcaCache'][self.tEvt['sType']] = self.toLCA(self.hsm[rTarget])
		# self.pp.pprint(self.rCurr); print
		self.exit(self.rCurr['toLcaCache'][self.tEvt['sType']])
		self.rNext = self.hsm[rTarget]

# 	# ==========================
# 	# non-cached version
# 	
# 	def stateTran(self, rTarget):
# 		toLca = self.toLCA(self.hsm[rTarget])
# 		self.exit(toLca)
# 		self.rNext = self.hsm[rTarget]

	# ====================================
	# Sets initial state.

	def stateStart(self, Target):
		self.rNext = self.hsm[Target]

	# ====================================
	# Gets current state.

	def stateCurrent(self):
		return self.rCurr

# ===================================================================

if __name__ == "__main__":

	# =========================================================
	# helpers

	import sys
	def printf(format, *args):
		sys.stdout.write(format % args)

	# 
	# ==============================================
	# Define event handlers for states. For clarity, the functions are named
	# for the states they handle (though this is not required). For details
	# see the UML state chart shown in the accompanying image
	# "hsmtst-chart.gif".

	# ====================================
	# Top/root state is state d.

	def top(self):
		if self.tEvt['sType'] == "init":
			# display event
			printf("top-%s;", self.tEvt['sType'])
			# transition to state d2.
			self.stateStart(d2)
			# returning a 0 indicates event was handled
			return 0
		elif self.tEvt['sType'] == "entry":
			# display event, do nothing 
			# else except indicate it was handled
			printf("top-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("top-%s;", self.tEvt['sType'])
			self.tEvt['nFoo'] = 0
			return 0
		elif self.tEvt['sType'] == "e":
			printf("top-%s;", self.tEvt['sType'])
			self.stateTran(d11)
			return 0
		return self.tEvt['sType']

	# ====================================                 

	def d1(self):
		if self.tEvt['sType'] == "init":
			printf("d1-%s;", self.tEvt['sType'])
			self.stateStart(d11)
			return 0
		elif self.tEvt['sType'] == "entry":
			printf("d1-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("d1-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "a":
			printf("d1-%s;", self.tEvt['sType'])
			self.stateTran(d1)
			return 0
		elif self.tEvt['sType'] == "b":
			printf("d1-%s;", self.tEvt['sType'])
			self.stateTran(d11)
			return 0
		elif self.tEvt['sType'] == "c":
			printf("d1-%s;", self.tEvt['sType'])
			self.stateTran(d2)
			return 0
		elif self.tEvt['sType'] == "f":
			printf("d1-%s;", self.tEvt['sType'])
			self.stateTran(d211)
			return 0
		return self.tEvt['sType']

	# 
	# ====================================

	def d11(self):
		if self.tEvt['sType'] == "entry":
			printf("d11-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("d11-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "d":
			printf("d11-%s;", self.tEvt['sType'])
			self.stateTran(d1)
			self.tEvt['nFoo'] = 0
			return 0
		elif self.tEvt['sType'] == "g":
			printf("d11-%s;", self.tEvt['sType'])
			self.stateTran(d211)
			return 0
		elif self.tEvt['sType'] == "h":
			printf("d11-%s;", self.tEvt['sType'])
			self.stateTran(top)
			return 0
		return self.tEvt['sType']

	# ====================================

	def d2(self):
		if self.tEvt['sType'] == "init":
			printf("d2-%s;", self.tEvt['sType'])
			self.stateStart(d211)
			return 0
		elif self.tEvt['sType'] == "entry":
			printf("d2-%s;", self.tEvt['sType'])
			if self.tEvt['nFoo'] != 0:
				self.tEvt['nFoo'] = 0
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("d2-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "c":
			printf("d2-%s;", self.tEvt['sType'])
			self.stateTran(d1)
			return 0
		elif self.tEvt['sType'] == "f":
			printf("d2-%s;", self.tEvt['sType'])
			self.stateTran(d11)
			return 0
		return self.tEvt['sType']

	# 
	# ====================================

	def d21(self):
		if self.tEvt['sType'] == "init":
			printf("d21-%s;", self.tEvt['sType'])
			self.stateStart(d211)
			return 0
		elif self.tEvt['sType'] == "entry":
			printf("d21-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("d21-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "a":
			printf("d21-%s;", self.tEvt['sType'])
			self.stateTran(d21)
			return 0
		elif self.tEvt['sType'] == "b":
			printf("d21-%s;", self.tEvt['sType'])
			self.stateTran(d211)
			return 0
		elif self.tEvt['sType'] == "g":
			printf("d21-%s;", self.tEvt['sType'])
			self.stateTran(d1)
			return 0
		return self.tEvt['sType']

	# ====================================

	def d211(self):
		if self.tEvt['sType'] == "entry":
			printf("d211-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "exit":
			printf("d211-%s;", self.tEvt['sType'])
			return 0
		elif self.tEvt['sType'] == "d":
			printf("d211-%s;", self.tEvt['sType'])
			self.stateTran(d21)
			return 0
		elif self.tEvt['sType'] == "h":
			printf("d211-%s;", self.tEvt['sType'])
			self.stateTran(top)
			return 0
		return self.tEvt['sType']

	# 
	# ==============================================
	# create HSM instance and populate it with states

	hsm = Hsm()

	# --------------------------------------------------------------------
	#             name                               parent's
	#              of              event             event
	#             state            handler           handler
	# --------------------------------------------------------------------
	hsm.addState ( "top",           top,               None)
	hsm.addState ( "d1",            d1,                top)
	hsm.addState ( "d11",           d11,               d1)
	hsm.addState ( "d2",            d2,                top)
	hsm.addState ( "d21",           d21,               d2)
	hsm.addState ( "d211",          d211,              d21)

	# ====================================
	# Interactive tester/explorer.

	def test_interactive():
		hsm.dump()
		print("\nInteractive Hierarchical State Machine Example")
		print "Miros revision: " + hsm.revision	+ "\n"
		print("Enter 'quit' to end.\n")
		# start/initialize HSM
		hsm.onStart(top)
		while True:
			# get letter of event
			sType = raw_input("\nEvent<-")
			if sType == "quit": return 
			if len(sType) != 1 or sType < "a" or sType > "h": 
				print "Event not defined.", 
			else:
				# dispatch event and display results
				hsm.onEvent(sType)

	# ====================================
	# Non-interactive test.

	def test_non_interactive():
		# hsm.dump()
		print("\nNon-interactive Hierarchical State Machine Example")
		print "Miros revision: " + hsm.revision	+ "\n"
		print("The following pairs of lines should all match each other and")
		print("the accompanying UML state chart 'hsmtst-chart.gif'.\n")
		# start/initialize HSM
		hsm.onStart(top)
		print("\ntop-entry;top-init;d2-entry;d2-init;d21-entry;d211-entry;\n")
		hsm.onEvent("a")
		print("\nd21-a;d211-exit;d21-exit;d21-entry;d21-init;d211-entry;\n")
		hsm.onEvent("b")
		print("\nd21-b;d211-exit;d211-entry;\n")
		hsm.onEvent("c")
		print("\nd2-c;d211-exit;d21-exit;d2-exit;d1-entry;d1-init;d11-entry;\n")
		hsm.onEvent("d")
		print("\nd11-d;d11-exit;d1-init;d11-entry;\n")
		hsm.onEvent("e")
		print("\ntop-e;d11-exit;d1-exit;d1-entry;d11-entry;\n")
		hsm.onEvent("f")
		print("\nd1-f;d11-exit;d1-exit;d2-entry;d21-entry;d211-entry;\n")
		hsm.onEvent("g")
		print("\nd21-g;d211-exit;d21-exit;d2-exit;d1-entry;d1-init;d11-entry;\n")
		hsm.onEvent("h")
		print("\nd11-h;d11-exit;d1-exit;top-init;d2-entry;d2-init;d21-entry;d211-entry;\n")
		hsm.onEvent("g")
		print("\nd21-g;d211-exit;d21-exit;d2-exit;d1-entry;d1-init;d11-entry;\n")
		hsm.onEvent("f")
		print("\nd1-f;d11-exit;d1-exit;d2-entry;d21-entry;d211-entry;\n")
		hsm.onEvent("e")
		print("\ntop-e;d211-exit;d21-exit;d2-exit;d1-entry;d11-entry;\n")
		hsm.onEvent("d")
		print("\nd11-d;d11-exit;d1-init;d11-entry;\n")
		hsm.onEvent("c")
		print("\nd1-c;d11-exit;d1-exit;d2-entry;d2-init;d21-entry;d211-entry;\n")
		hsm.onEvent("b")
		print("\nd21-b;d211-exit;d211-entry;\n")
		hsm.onEvent("a")
		print("\nd21-a;d211-exit;d21-exit;d21-entry;d21-init;d211-entry;\n")

	#  ====================================

	# test_interactive()
	test_non_interactive()

	raw_input("Press return to continue")
