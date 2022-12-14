import csv

def write_csv_file(name, row, rows):
	csv_file = open(f'{name}.csv', 'w')   
	csv_write = csv.writer(csv_file)
	csv_write.writerow(row)
	csv_write.writerows(rows)
	csv_file.close()

def digit_cleaner(num):
        num = str(num)
        r_num = num.replace(' ', '')
        l_num = [n for n in num]
        r = []

        for x in l_num:
            if x.isdigit() and x:
                r.append(x)
            elif not x:
                r.append('None')

        return ''.join(r)
