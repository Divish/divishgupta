import collections
# import bisect

# Some variable that stores whether I want to sort final list or final value
def update_dictionary(dictionary_name, to_update):
    if isinstance(to_update, collections.Mapping):
        for key, value in to_update.iteritems():
            temp = update_dictionary(dictionary_name.get(key, {}), value)
            dictionary_name[key] = temp
    elif isinstance(to_update, collections.Iterable) and not isinstance(to_update, basestring):
        for elements in to_update:
            temp = update_dictionary(elements, elements)
            elements = temp
    else:
        if isinstance(dictionary_name, collections.Mapping):
            for keys in dictionary_name.keys():
                dictionary_name[keys] = to_update
        else:
            dictionary_name.append(to_update)
    #
    #
    #
    #
    #         dictionary_name[key] = temp
    #         elif isinstance(value, collections.Iterable) and not isinstance(value, basestring):
    #
    #
    #
    #
    #
    # else:
    #
    #             if sort == 0 and nested_list == 1:
    #                 #print '1'
    #                 if key in dictionary_name.keys():
    #                     dictionary_name[key] = [dictionary_name[key]]
    #                     dictionary_name[key].append(to_update[key])
    #                 else:
    #                     dictionary_name[key] = to_update[key]
    #             else:
    #                 if key in dictionary_name.keys():
    #                     dictionary_name[key] = [dictionary_name[key]]
    #                     bisect.insort(dictionary_name[key], to_update[key])
    #                 else:
    #                     dictionary_name[key] = to_update[key]
    #     return dictionary_name

x = {}
y = {2:{1: 5}}
z = {2: {1: 6}}

update_dictionary(x,y)
print x
update_dictionary(x,z)
print x