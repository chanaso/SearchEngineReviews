import re
import os
import shutil
import timeit
import copy

start = timeit.default_timer()

# list size
REVIEW_DETAILS_LEN = 5
DICTIONARY_DETAILS_LEN = 3

# write binary data
CONST_63 = 63
CONST_16383 = 16383
CONST_4194303 = 4194303
CONST_1073741823 = 1073741823

BIN_CONST_16 = 16384
BIN_CONST_41 = 8388608
BIN_CONST_10 = 3221225472

# read binary data
CONST_64 = 64
CONST_128 = 128
CONST_192 = 192

# buffer size
MAX_REVIEWS = 300000
MAX_MEMORY_SIZE = 400000000

class IndexWriter:

#------------init-----------------------------------------------------------------------------------------------------------
    def __init__(self):
        self.dictionary = []
        self.dictionary_details = []
        self.posting_list = []
        self.review_details = []
        self.extract_details = []
        self.last_offset = []
        self.last_review_id = 1
        self.dictionary_details_len = 3

#------------extract information from the given file-----------------------------------------------------------------------------------------------------------

    def extractReviewDetails(self, reader, review_id):
        # with open(inputFile, "r") as reader:  # read the reviews file
        extract_details_list = []
        counter = 0

        for line in reader:
            if line.startswith('product/productId: ', 0):  # extract productId
                productId = line.replace('product/productId: ', '')
                productId = productId[:-1]
                self.extract_details.append(review_id)
                self.extract_details.append(productId)

            if line.startswith('review/helpfulness:', 0):  # extract helpfulness
                helpfulness = line.replace('review/helpfulness: ', '')
                helpfulness = helpfulness[:-1]
                self.extract_details.append(helpfulness)

            if line.startswith('review/score:', 0):  # extract score
                score = line.replace('review/score: ', '')
                score = int(float(score[:-1]))
                self.extract_details.append(score)

            if line.startswith('review/text: ', 0):  # extract review text and text lenght
                text = line.replace('review/text: ', '')
                text = text[:-1]
                text = text.lower()  # normalize the review text
                text = text.replace('_', ' ')
                text = re.sub(r'\W+', ' ', text)
                tokens = text.split()
                self.extract_details.append(len(tokens))
                self.extract_details.append(tokens)

                if len(self.extract_details) == 6:
                    extract_details_list.append(self.extract_details)
                    counter+=1
                self.extract_details = []
                review_id = review_id + 1

                if counter >= MAX_REVIEWS:
                    extract_details_list.append(0)
                    return extract_details_list
        extract_details_list.append(1)
        return extract_details_list

#------------convert ins to binary data by secound methode we learned-----------------------------------------------------------------------------------------------------------
    def writeToBinaryFile(self, file, list_to_write):
        for number in list_to_write:  # write to binary file the posting list
            if number <= CONST_63:  # first bits 00
                file.write(number.to_bytes(1, 'big'))
            elif number > CONST_63 and number <= CONST_16383:  # first bits 01
                number = number | BIN_CONST_16  # add to number the identifier bits
                file.write(number.to_bytes(2, 'big'))
            elif number > CONST_16383 and number <= CONST_4194303:  # first bits 10
                number = number | BIN_CONST_41  # add to number the identifier bits
                file.write(number.to_bytes(3, 'big'))
            elif number > CONST_4194303 and number <= CONST_1073741823:  # first bits 11
                number = number | BIN_CONST_10  # add to number the identifier bits
                file.write(number.to_bytes(4, 'big'))

#------------read from file and covert binary data to ints-----------------------------------------------------------------------------------------------------------
    def readFromBinaryFile(self, file, list_len):
        numbers_list = []

        while True:
            if list_len != 0 and list_len == len(numbers_list):
                return numbers_list
            byte = file.read(1)  # read one byte from file
            if not byte:  # end of rhe file
                return numbers_list
            int_byte = int.from_bytes(byte, 'big')  # convert byte to int
            first_bit = (int_byte >> 7 & 1)  # first bit of byte
            sec_bit = (int_byte >> 6 & 1)  # secound bit of byte

            if (first_bit == 0 and sec_bit == 0):  # case 00
                numbers_list.append(int_byte)

            if (first_bit == 0 and sec_bit == 1):  # case 01
                byte2 = file.read(1)  # read another byte
                byte = (int_byte ^ CONST_64)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))

            if (first_bit == 1 and sec_bit == 0):  # case 10
                byte2 = file.read(2)  # read another byte
                byte = (int_byte ^ CONST_128)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))

            if (first_bit == 1 and sec_bit == 1):  # case 11
                byte2 = file.read(3)  # read another byte
                byte = (int_byte ^ CONST_192)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))


#------------fix the gap list to be accurate data -----------------------------------------------------------------------------------------------------------
    def gapFunction(self, gap_list): # gets strs list of gaps and return the real offset
        new_list = []
        tmp_var = 0
        for i in range(0, len(gap_list)):
            new_list.append(int(gap_list[i]) + tmp_var)
            tmp_var = int(gap_list[i]) + tmp_var
        return new_list

#------------return list from binary file-----------------------------------------------------------------------------------------------------------
    def  getListByOffset(self, file, setSeek, list_len):
        file.seek(setSeek)  # set the pointer of file to be in the last review
        return self.readFromBinaryFile(file, list_len) # set number of reviews

#------------create index file for reviews-----------------------------------------------------------------------------------------------------------
    def createFileReviewDetails(self, dir, review_details, fileName):
        reviewDetails = []
        offset_list = []
        with open(dir + "/indexReviewDetails" + fileName + ".bin", "wb") as reviewDetailsFile:
            with open(dir + "/indexReviewOffset" + fileName + ".bin", "wb") as offsetFile:
                offset_list.append(reviewDetailsFile.tell()) # save the first offset of id review
                pre_offset = reviewDetailsFile.tell() # save the first offset of id review in prev

                for detail in review_details:
                    reviewDetails.append(int(detail[0])) # add id review to list
                    list = detail[2].split('/') # divid helpfulness to 2 integers (clear '\')

                    reviewDetails.append(int(list[0])) # add the helpfullness to list
                    reviewDetails.append(int(list[1]))
                    reviewDetails.append(int(detail[3])) # add the score to list
                    reviewDetails.append(int(detail[4])) # add count of tokens in the current rev to list

                    self.writeToBinaryFile(reviewDetailsFile, reviewDetails) # write the  rev details list to file
                    offset_list.append(reviewDetailsFile.tell() - pre_offset) # save the seek of id rev in gaps to offset file
                    pre_offset = reviewDetailsFile.tell() # save pre offset for the next gap
                    reviewDetails = []
                    list = []
                self.last_offset.append(offset_list[-1])
                self.writeToBinaryFile(offsetFile, offset_list[:-1]) # write list of offsets to file

#------------create index files for products-----------------------------------------------------------------------------------------------------------
    def createFileProductsids(self, dir, review_id, product_id, fileName):
        products_dict = {}
        products = []

        for i in range (0, len(review_id)):
            if product_id[i] not in products_dict:
                products.append(review_id[i]) # add the review with this product id
                products_dict[product_id[i]] = products
                products = []
            else:
                products_dict[product_id[i]].append(review_id[i])

        with open(dir + "/indexProductsIds" + fileName + ".txt", "w") as productFile:
            for key, value in products_dict.items():
                productFile.write(key)
                value = self.makeGaps(value)

                for val in value:
                    productFile.write(' ' + str(val))
                productFile.write('\n')
        self.last_review_id = review_id[len(review_id)-1] + 1


#------------create index files for dicitionary-----------------------------------------------------------------------------------------------------------
    def createFilesDicionary(self, dir, dict_list, reviewIds, fileName):
        review_dict = {}
        list_dict = []
        dict_structer = {}
        dictionaty_info = []
        sum = 0
        for id in range(len(reviewIds)):  # build list with dictionaries to every review words. each dictionary contain words with frequency
            for word in dict_list[id]:
                if word not in review_dict:
                    review_dict[word] = 1
                else:
                    review_dict[word] += 1
            list_dict.append(review_dict)
            review_dict = {}

        for d in range(len(reviewIds)):  # build dictionary, for each word reviewid + frequency
            for key in list_dict[d]:
                if key not in dict_structer:
                    dict_structer[key] = []
                    dict_structer[key].append(reviewIds[d])
                    dict_structer[key].append((list_dict[d])[key])
                else:
                    dict_structer[key].append(reviewIds[d])
                    dict_structer[key].append((list_dict[d])[key])
        dict_struct = {}
        # sort dictionary
        for key in sorted(dict_structer.keys()):
            dict_struct[key] = dict_structer[key]

        with open(dir + "/indexDictionary" + fileName + ".txt", "w") as indexDictionaryFile:
            with open(dir + "/indexDictionaryDetails" + fileName + ".bin", "wb") as indexDictionaryDetailsFile:
                with open(dir + "/indexPostingList" + fileName + ".bin", "wb") as indexPostingListFile:
                    first_key = next(iter(dict_struct))
                    for key, value in dict_struct.items():
                        for i in range(len(value)):
                            if i%2 != 0:
                                sum+= value[i] # save the frequency of the token in all the reviews

                        gap_posting = indexPostingListFile.tell() # save current place in postingList.
                        dictionaty_info.append(sum)
                        sum = 0

                        for index in range(0, len(value)-1): # sets the postinglist ids to gaps
                            if index%2 == 0:
                                if index == 0:
                                    prev_val = value[index]
                                else:
                                    tmp = value[index]
                                    value[index] = tmp - prev_val
                                    prev_val = tmp

                        if first_key == key: # first key
                            indexDictionaryFile.write(key + ' ')
                            indexDictionaryFile.write(str(indexDictionaryDetailsFile.tell()) + ' ')
                            gap_dict = indexDictionaryDetailsFile.tell()  # save current place in dictioanryDetails.

                        elif first_key != key:
                            indexDictionaryFile.write(key + ' ')  # write to dictionary
                            indexDictionaryFile.write(str(indexDictionaryDetailsFile.tell() - gap_dict)+ " ")
                            gap_dict = indexDictionaryDetailsFile.tell()

                        self.writeToBinaryFile(indexPostingListFile, value) # write current key(token) postinglist to file
                        # write to dictDetails file: sum, offset to pstingList, lenght of posingList
                        dictionaty_info.append(gap_posting)
                        dictionaty_info.append(len(value))
                        self.writeToBinaryFile(indexDictionaryDetailsFile, dictionaty_info)
                        dictionaty_info = []

#------------return posting list from file-----------------------------------------------------------------------------------------------------------
    def getPostingList(self, dictionary, key, dir, indexDictionaryDetails, indexPostingList):
        try:
            seek_to_details = dictionary[key]

            # extract the offset to posting list from indexDictionaryDetails file
            with open(dir + "/" + indexDictionaryDetails, "rb") as reader:
                offset = self.getListByOffset(reader, seek_to_details, self.dictionary_details_len)[1]
                posting_list_len = self.getListByOffset(reader, seek_to_details, self.dictionary_details_len)[2]

            # extract posting list
            with open(dir + "/" + indexPostingList, "rb") as reader:
                posting_list = self.getListByOffset(reader, offset, posting_list_len)
                id_gaps = self.gapFunction(self.getListByOffset(reader, offset, posting_list_len)[::2]) # fix the gaps of ids in posting list

                # combine between rev ids(accurate ids! not gaps) and freq lists
                result = [None] * (len(posting_list))
                result[0::2] = id_gaps
                result[1::2] = posting_list[1::2]
                return result

        except:
            return []


#------------return file num of smallest token in the list(by abc order)-----------------------------------------------------------------------------------------------------------

    def firstTokenAbcOrder(self, tokens_list):
        new_tokens_list = sorted(tokens_list)
        new_tokens_list = list(filter(None, new_tokens_list))  # fastest
        return tokens_list.index(new_tokens_list[0])

#------------fix gaps in the posting list-----------------------------------------------------------------------------------------------------------

    def fixPostingGaps(self, posting_list):
        tokens_keys = self.gapFunction(posting_list[0::2])  # fix the gaps
        pointers_values = posting_list[1::2]
        for i, v in enumerate(pointers_values):
            tokens_keys.insert(2 * i + 1, v)
        return tokens_keys  # bulid the tokens and pointers to one acurate list

#------------merge indexs-----------------------------------------------------------------------------------------------------------
    def mergeDictionary(self, dictionary_files_list):
        dir = dictionary_files_list[0]
        file_number = int(dictionary_files_list[1])
        dictionary_files_list.remove(dictionary_files_list[0])
        dictionary_files_list.remove(dictionary_files_list[0])
        list_of_dicts = []
        flag = True

        # puts all the files dictionary in the list of dictionarys
        for i in range(0, len(dictionary_files_list), 3):
            with open(dir + '/' + dictionary_files_list[i], 'r') as file1:
                line = file1.readlines()[0].split(' ')
                tokens_keys = line[0::2]
                pointers_values = self.gapFunction(line[1::2])  # fix the gaps
                list_of_dicts.append(dict(zip(tokens_keys, pointers_values))) # bulid the tokens and pointes dictionary)
            os.remove(dir + '/' + dictionary_files_list[i])

        # open new files for big index and open dictionary and postinflist files
        fileList = []
        for i in range(1, len(dictionary_files_list)):
            if i%3 != 0:
                fileList.append(dictionary_files_list[i])

        fileDetailsList = fileList[0::2]
        filePostingList = fileList[1::2]

        readerDetailsList = [None]*len(fileDetailsList)
        readerPostingList = [None]*len(filePostingList)

        # open index files to read from
        for i in range(0, len(fileDetailsList)):
            readerDetailsList[i] = open(dir + '/' + fileDetailsList[i], 'rb')
            readerPostingList[i] = open(dir + '/' + filePostingList[i], 'rb')

        with open(dir + '/indexDictionary.txt', 'w') as dictWriter, \
             open(dir + '/new_indexDictionaryDetails.bin', 'wb') as detailsWriter, \
             open(dir + '/new_indexPostingList.bin', 'wb') as postingListWriter:

            while flag:
                tokens_list = []
                offset_list = []
                check = False
                for i in range(len(list_of_dicts)):
                    d = list_of_dicts[i]
                    if not d:
                        tokens_list.append('')
                        offset_list.append(-1)
                    else:
                        tokens_list.append(next(iter(d)))
                        offset_list.append(d[next(iter(d))])

                index = self.firstTokenAbcOrder(tokens_list) # index of dictionary of the first word in all dictionarys

                # initilaize the gaps
                if dictWriter.tell() == 0:
                    gap_dict = detailsWriter.tell()
                if postingListWriter.tell() == 0:
                    gap_posting = postingListWriter.tell()

                details1 = self.getListByOffset(readerDetailsList[index], list_of_dicts[index][tokens_list[index]], DICTIONARY_DETAILS_LEN) # save token details
                posting_list1 = self.getListByOffset(readerPostingList[index], details1[1], details1[2])
                if len(posting_list1) > 2:
                    posting_list1 = self.fixPostingGaps(posting_list1)

                details = []
                token_freq = 0
                token_freq += details1[0]
                posting_list_len = 0
                posting_list_len += details1[2]
                posting_list = posting_list1

                for i in range(len(tokens_list)): # theres is token that shows in two dictionarys or more
                    if tokens_list[i] == tokens_list[index] and i != index:
                        offset = list_of_dicts[i][tokens_list[i]]
                        details2 = self.getListByOffset(readerDetailsList[i], offset, DICTIONARY_DETAILS_LEN) # save token details
                        token_freq += details2[0]
                        posting_list_len += details2[2]

                        posting_list2 = self.getListByOffset(readerPostingList[i], details2[1], details2[2])
                        if len(posting_list2) > 2:
                            posting_list2 = self.fixPostingGaps(posting_list2)
                        posting_list = posting_list + posting_list2

                # write to files
                dictWriter.write(tokens_list[index] + ' ')
                dictWriter.write(str(detailsWriter.tell() - gap_dict) + ' ')
                gap_dict = detailsWriter.tell()

                details.append(token_freq)
                details.append(postingListWriter.tell())
                details.append(posting_list_len)
                gap_posting = postingListWriter.tell()
                self.writeToBinaryFile(detailsWriter, details)

                for i in range(len(list_of_dicts)): # delete token from the dictionarys
                    d = list_of_dicts[i]
                    if tokens_list[index] in d:
                        del d[str(tokens_list[index])]

                if len(posting_list) > 2:
                    for index in range(0, len(posting_list) - 1):  # sets the postinglist ids to gaps
                        if index % 2 == 0:
                            if index == 0:
                                prev_val = posting_list[index]
                            else:
                                tmp = posting_list[index]
                                posting_list[index] = tmp - prev_val
                                prev_val = tmp

                self.writeToBinaryFile(postingListWriter, posting_list)

                counter = 0
                for i in range(len(list_of_dicts)):
                    d = list_of_dicts[i]
                    if not d:
                        counter+= 1

                if counter == len(list_of_dicts):
                    flag = False

        for i in range(0, len(fileDetailsList)):
            readerDetailsList[i].close()
            readerPostingList[i].close()
            os.remove(dir + '/' + fileDetailsList[i])  # delete file2
            os.remove(dir + '/' + filePostingList[i])

        os.rename(dir + '/new_indexDictionaryDetails.bin', dir + '/indexDictionaryDetails.bin')
        os.rename(dir + '/new_indexPostingList.bin', dir + '/indexPostingList.bin')

#------------ build gaps list-----------------------------------------------------------------------------------------------------------
    def makeGaps(self, list):
        new_list = []
        for val in list:
            if list.index(val) == 0:
                new_list.append(val)
                prev_val = val
            else:
                new_list.append(int(val) - int(prev_val))
                prev_val = val
        return new_list

#------------ merge products files-----------------------------------------------------------------------------------------------------------
    def mergeProductsId(self, dir, indexProductsIds, indexProductsIds1):

        #  save the first file to dictionary: key = productid, value = reviewid list
        dict = {}
        with open(dir + '/' + indexProductsIds, 'r') as file1:
            for line in file1:
                list = line.strip('\n').split(' ')  # remove \n
                product_id = list[0]

                if len(list) > 2:
                    dict[product_id] = self.gapFunction(list[1:])
                else:
                    dict[product_id] = int(list[1])

        with open(dir + '/' + indexProductsIds, 'w') as file1:
            with open(dir + '/' + indexProductsIds1, 'r') as file2:
                for line1 in file2:

                    # go through every line in the sec file
                    list = line1.strip('\n').split(' ')  # remove \n
                    product_id1 = list[0]
                    list = list[1:]

                    # productid in file 2 not shwoing in file1 and write it to the main index file
                    if product_id1 not in dict.keys():
                        file1.write(product_id1)
                        if len(list) > 1:
                            for val in list:
                                file1.write(' ' + str(val))
                        else:
                            file1.write(' ' + str(list[0]))
                        file1.write('\n')

                    else:
                        #  same productid shows in both files, write the product to the file and update the ids review list
                        for key, value in dict.items():
                            if product_id1 == key:
                                file1.write(key)
                                new_list = self.gapFunction(list)
                                if not isinstance(value, int):
                                    new_list = self.makeGaps(value + new_list)
                                else:
                                    tmp_list = []
                                    tmp_list.append(value)
                                    new_list = self.makeGaps(tmp_list + new_list)
                                for val in new_list:
                                    file1.write(' ' + str(val))
                                file1.write('\n')
                                dict[key] = 'copied'

                # shows in file 1 and not in file 2 and write it to the main index file
                for key, value in dict.items():
                    if dict[key] != 'copied':
                        file1.write(key)
                        if not isinstance(value, int):
                            value = self.makeGaps(value)
                            for val in value:
                                file1.write(' ' + str(val))
                        else:
                            file1.write(' ' + str(value))
                        file1.write('\n')
        os.remove(dir + '/' + indexProductsIds1)  # delete file2

#------------merge reviews files work!!!-----------------------------------------------------------------------------------------------------------
    def mergeReview(self, dir, indexReviewOffset, indexReviewOffset1, indexReviewDetails, indexReviewDetails1):
        with open(dir + '/' + indexReviewOffset, 'ab') as file1:
            with open(dir + '/' + indexReviewOffset1, 'rb') as file2:

                byte = file2.read(1) # change the first byte(offset) in the indexReviewOffset file to the real offset
                byte = self.last_offset[0]
                del self.last_offset[0]

                file1.write(byte.to_bytes(1, 'big')) # write the first real offset to the main index file
                file1.write(file2.read()) # append file of indexReviewOffset1 to indexReviewOffset
        os.remove(dir + '/' + indexReviewOffset1) # delete file2

        with open(dir + '/' + indexReviewDetails, 'ab') as file1:
            with open(dir + '/' + indexReviewDetails1, 'rb') as file2:
                file1.write(file2.read()) # append file2 to file1
        os.remove(dir + '/' + indexReviewDetails1) # delete file2

#------------send all the file index to merge-----------------------------------------------------------------------------------------------------------
    def mergeIndex(self, dir, file_number):
        dictionary_files_list = []
        dictionary_files_list.append(dir)
        dictionary_files_list.append(file_number)
        dictionary_files_list.append('indexDictionary.txt')
        dictionary_files_list.append('indexDictionaryDetails.bin')
        dictionary_files_list.append('indexPostingList.bin')

        for i in range(1,int(file_number)): # send files to merge
            dictionary_files_list.append('indexDictionary' + str(i) + '.txt')
            dictionary_files_list.append('indexDictionaryDetails' + str(i) + '.bin')
            dictionary_files_list.append('indexPostingList' + str(i) + '.bin')
            self.mergeProductsId(dir , 'indexProductsIds.txt', 'indexProductsIds' + str(i) + '.txt')
            self.mergeReview(dir, 'indexReviewOffset.bin', 'indexReviewOffset' + str(i) + '.bin', 'indexReviewDetails.bin', 'indexReviewDetails' + str(i) + '.bin')
        self.mergeDictionary(dictionary_files_list)

#------------build index for big data-----------------------------------------------------------------------------------------------------------
    def buildBigIndex(self, dir, inputFile):
        with open(inputFile, "r") as reader:
            file_number = ''

            while True:  # read the whole file
                index_details = self.extractReviewDetails(reader, self.last_review_id)
                last_var = index_details[len(index_details) - 1]

                if last_var == 1 and len(index_details) == 1: # finished to read the file
                    break

                index_details = index_details[:-1]
                index_details_tmp = index_details
                index_details_tmp1 = copy.deepcopy(index_details)

                dict_list = [j.pop(-1) for j in index_details_tmp]

                # build files
                self.createFileReviewDetails(dir, index_details_tmp, file_number)  # make review details file
                self.createFileProductsids(dir, [j.pop(0) for j in index_details], [j.pop(0) for j in index_details], file_number)  # make file to products id, send id_r and products_id
                self.createFilesDicionary(dir, dict_list, [j.pop(0) for j in index_details_tmp1], file_number)  # create 3 files: 1. dicionary+gaps, 2.dict detail


                # review_id = review_id + MAX_REVIEWS
                if file_number == '':
                    file_number = '1'
                else:
                    file_number = str(int(file_number) + 1)

                if last_var == 1: # finished to read the file
                    break
                # if last_var == 1 and review_id % MAX_REVIEWS != 0:
                #     break


            self.mergeIndex(dir ,file_number)

#------------write the indexs-----------------------------------------------------------------------------------------------------------
    def write(self, inputFile, dir):
        if os.path.exists(dir):  # if directory exist remove it
            shutil.rmtree(dir)
        os.makedirs(dir)  # create directory

        if (os.path.getsize(inputFile) > MAX_MEMORY_SIZE):  # file is too large needs to split the build
            self.buildBigIndex(dir, inputFile)

        else:  # the index build is as normal
            with open(inputFile, "r") as reader:
                index_details = self.extractReviewDetails(reader, 1)[:-1] # a list that contain all the needed details
            index_details_tmp = index_details
            index_details_tmp1 = copy.deepcopy(index_details)

            dict_list = [j.pop(-1) for j in index_details_tmp]

            # build files
            self.createFileReviewDetails(dir, index_details_tmp, '') # make review details file
            self.createFileProductsids(dir, [j.pop(0) for j in index_details], [j.pop(0) for j in index_details], '') # make file to products id, send id_r and products_id
            self.createFilesDicionary(dir, dict_list, [j.pop(0) for j in index_details_tmp1], '') # create 3 files: 1. dicionary+gaps, 2.dict details, 3.posting list

    def removeIndex(self, dir):
        if os.path.exists(dir):  # if directory exist remove it
            shutil.rmtree(dir)


m = IndexWriter()
m.write('Reviews/Books100000.txt', 'indexFiles')
end = timeit.default_timer()
print(end - start)