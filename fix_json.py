import json

def reformat_json_file(input_file, output_file):
    with open(input_file, 'r') as file:
        content = file.read()

    # Adding '[' at the beginning and ']' at the end
    json_content = '[' + content.replace('}\n{', '},\n{') + ']'

    with open(output_file, 'w') as file:
        file.write(json_content)


reformat_json_file('/home/sara/ArXivData/duplicate_publications.json', '/home/sara/ArXivData/reformatted_duplicate_publications.json')
