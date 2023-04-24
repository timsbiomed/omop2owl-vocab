.PHONY: all


# MAIN COMMANDS / GOALS ------------------------------------------------------------------------------------------------
all: n3c.db

# TODO: Dockerized SemSQL I think is best way to do this
n3c.db: n3c.owl
	echo TODO: Convert OWL to SemSQL

n3c.owl: io/input/termhub-csets
	 python3 -m n3c_owl_ingest

# TODO: allow some force updating of termhub-csets
io/input/termhub-csets:
	cd io/input; git clone https://github.com/jhu-bids/termhub-csets.git

# SETUP / INSTALLATION -------------------------------------------------------------------------------------------------
install:
	pip install -r requirements.txt

# HELP -----------------------------------------------------------------------------------------------------------------
help:
	@echo "-----------------------------------"
	@echo "	Command reference: N3C OWL Ingest"
	@echo "-----------------------------------"
	@echo "all"
	@echo "Creates all release artefacts.\n"
	@echo "n3c.owl"
	@echo "Creates OWL artefact: n3c.owl\n"
	@echo "n3c.db"
	@echo "Creates SemanticSQL sqlite artefact: n3c.db\n"
	@echo "install"
	@echo "Install's Python requirements.\n"
