import random
import math
import multiprocessing as mp
import time

def mergeSort(array):
    # Si el array solo contiene un elemento, lo devolvemos
    if len(array) <= 1:
        return array

    # Sacamos el punto medio del array para volver a llamar a la funcion
    # con cada mitad hasta que devuelve un elemento de cada mitad
    # y llama a merge con estos.
    mid = int(len(array) / 2)

    left, right = mergeSort(array[:mid]), mergeSort(array[mid:])

    return merge(left, right)


def merge(*args):
    # Admite argumentos izquierda y derecha, y tambien una tupla 
    # de dos elementos, que funciona mucho mejor con multiprocesamiento.
    left, right = args[0] if len(args) == 1 else args

    # Creamos el array en el que se va a ir metiendo los numeros ordenados
    # Tambien creamos los indices con los que nos iremos moviendo y tambien
    # posicionando los numeros
    result = []
    leftIndex = rightIndex = 0

    # Mientras estamos dentro del array, va a ir colocando a la izquierda los menores
    # y a la derecha los mayores, a la vez que incrementamos el indice cuando colocamos
    # un numero en dicho lado
    while leftIndex < len(left) and rightIndex < len(right):
        if left[leftIndex] < right[rightIndex]:
            result.append(left[leftIndex])
            leftIndex += 1

        else:
            result.append(right[rightIndex])
            rightIndex += 1

    result.extend(left[leftIndex:])
    result.extend(right[rightIndex:])

    return result

def mergeSortParallel(data):

    # Primero creamos un pool con los procesos que van a trabajar, uno por cada core de CPU.
    # Despues dividimos el array inicial en particiones del mismo tamaño para cada proceso.
    # Por ultimo llamamos a la funcion mergeSort para cada particion.
    processes = mp.cpu_count()
    pool = mp.Pool(processes=processes)
    size = int(math.ceil(float(len(data)) / processes))
    data = [data[i * size:(i + 1) * size] for i in range(processes)]
    data = pool.map(mergeSort, data)

    # Ahora que cada particion esta ordenada, las uniremos usando el pool
    # hasta que tengamos un solo resultado ordenado.
    while len(data) > 1:
        # Si el numero de particiones restantes es impar, sacaremos la ultima y la meteremos
        # de nuevo tras la primera iteracion del loop, ya que solo nos interesan las parejas
        # de particiones a unir
        if len(data) % 2 == 1:
            extra = data.pop() 
        else: 
            extra = None
        
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data) + ([extra] if extra else [])

    return data[0]

if __name__ == "__main__":
    
    array = [random.randint(0,256) for i in range(21765304)]
    print("array creado")

    inicioS = time.time()
    result = mergeSort(array)
    finS = time.time()

    print("Secuencial:")
    #print(result)
    print(finS-inicioS)

    inicioP = time.time()
    result = mergeSortParallel(array)
    finP = time.time()

    print("Paralelo:")
    #print(result)
    print(finP - inicioP)