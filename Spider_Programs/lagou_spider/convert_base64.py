import base64


def convert_base64(Img_address):
    with open(Img_address, 'rb') as f:
        base64_data = base64.b64encode(f.read())
        return base64_data
