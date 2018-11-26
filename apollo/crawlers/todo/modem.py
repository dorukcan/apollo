import datetime
import telnetlib
from pprint import pprint

from slugify import slugify


class Modem:
    HOST = "192.168.1.1"
    USER = "admin"
    PASSWORD = "admin"

    def __init__(self):
        self.tn = None

    def send_command(self, command):
        self.tn.write((command + "\n").encode())
        print(command + " ok")

    def run(self):
        self.login()
        self.send_command("arp show")
        self.send_command("lanhosts show all")
        self.logout()

        data = self.crawl_data()
        self.tn.close()

        # utils.save_data(data, "misc.modem")
        pprint(data)

        return data

    def login(self):
        self.tn = telnetlib.Telnet(self.HOST)
        print("Connection ok")

        self.tn.read_until(b"Login: ")
        self.tn.write(self.USER.encode('ascii') + b"\n")
        print("Username ok")

        self.tn.read_until(b"Password: ")
        self.tn.write(self.PASSWORD.encode('ascii') + b"\n")
        print("Password ok")

    def logout(self):
        self.send_command("exit")
        print("Logout ok")

    def read_output(self):
        return self.tn.read_all().decode()

    def crawl_data(self):
        raw_data = self.read_output()
        print("Output ok")
        data = self.extract_data(raw_data)
        print("Extraction ok")

        return data

    def extract_data(self, data):
        data = [row.strip() for row in data.split("\r\n") if row.strip()]

        result = {}

        current_command = None
        command_output = None

        for row in data:
            if row[0] == '>':
                if current_command:
                    result[current_command] = command_output

                current_command = row.replace("> ", "")
                command_output = []
            else:
                current_row = row.split("  ")
                if len(current_row) > 1:
                    command_output.append([column.strip() for column in current_row if column])

        final_result = []
        created_at = datetime.datetime.now()

        for command, output in result.items():
            keys = output[0]
            named_output = []

            for row in output[1:]:
                values = {
                    "command": command,
                    "created_at": created_at
                }

                for key, column in zip(keys, row):
                    values[slugify(key, separator="_")] = column

                named_output.append(values)

            final_result.extend(named_output)

        return final_result


if __name__ == "__main__":
    Modem().run()
