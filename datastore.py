class Datastore:
    def __init__(self):
        self.blocks = []

    def append_block(self, transactions):
        sequence_number = len(self.blocks) + 1
        block = (sequence_number, transactions)
        self.blocks.append(block)

    def get_last_sequence_number(self):
        return len(self.blocks)

    def get_transactions(self):
        return [transaction for _, transactions in self.blocks for transaction in transactions]